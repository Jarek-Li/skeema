package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/tengo"
)

// DDLStatement represents a DDL SQL statement (CREATE TABLE, ALTER TABLE, etc).
// It may represent an external command to shell out to, or a DDL statement to
// run directly against a DB.
type DDLStatement struct {
	// Err represents errors that occur from applying statement modifiers (which
	// can forbid destructive DDL), from building an external command string (which
	// could reference invalid template variables), or from executing the DDL
	// (errors from the DB directly, or a nonzero exit code from an external
	// command)
	Err error

	stmt     string
	shellOut *ShellOut

	instance   *tengo.Instance
	schemaName string
}

// NewDDLStatement creates and returns a DDLStatement. It may return nil if
// the StatementModifiers cause it to be a no-op. In the case of an error
// constructing the statement (mods disallowing destructive DDL, invalid
// variable interpolation in --alter-wrapper, etc), a non-nil DDLStatement will
// still be returned, but its Err field will be non-nil, preventing any
// execution of the DDLStatement.
func NewDDLStatement(diff tengo.TableDiff, mods tengo.StatementModifiers, target *Target) *DDLStatement {
	ddl := &DDLStatement{
		instance:   target.Instance,
		schemaName: target.SchemaFromDir.Name,
	}
	var err error

	// Look up size of affected table. This will be 0 for CREATE TABLE statements.
	var tableName string
	var tableSize int64
	switch diff := diff.(type) {
	case tengo.AlterTable:
		tableSize, err = ddl.getTableSize(target, diff.Table)
		tableName = diff.Table.Name
	case tengo.DropTable:
		tableSize, err = ddl.getTableSize(target, diff.Table)
		tableName = diff.Table.Name
	case tengo.CreateTable:
		tableName = diff.Table.Name
		err = nil
	}
	ddl.setErr(err)

	// If --safe-below-size option in use, enable additional statement modifier
	// if the table's size is less than the supplied option value
	safeBelowSize, err := target.Dir.Config.GetBytes("safe-below-size")
	ddl.setErr(err)
	if ddl.Err == nil && tableSize < int64(safeBelowSize) {
		mods.AllowUnsafe = true
		log.Debugf("Allowing unsafe operations for table %s: size=%d < safe-below-size=%d", tableName, tableSize, safeBelowSize)
	}

	// Options may indicate some/all DDL gets executed by shelling out to another program.
	wrapper := target.Dir.Config.Get("ddl-wrapper")
	if _, isAlter := diff.(tengo.AlterTable); isAlter && target.Dir.Config.Changed("alter-wrapper") {
		minSize, err := target.Dir.Config.GetBytes("alter-wrapper-min-size")
		ddl.setErr(err)
		if tableSize >= int64(minSize) {
			wrapper = target.Dir.Config.Get("alter-wrapper")

			// If alter-wrapper-min-size is set, and the table is big enough to use
			// alter-wrapper, disable --alter-algorithm and --alter-lock. This allows
			// for a configuration using built-in online DDL for small tables, and an
			// external OSC tool for large tables, without risk of ALGORITHM or LOCK
			// clauses breaking expectations of the OSC tool.
			if minSize > 0 {
				log.Debugf("Using alter-wrapper for table %s: size=%d >= alter-wrapper-min-size=%d", tableName, tableSize, minSize)
				if mods.AlgorithmClause != "" || mods.LockClause != "" {
					log.Debug("Ignoring --alter-algorithm and --alter-lock for generating DDL for alter-wrapper")
					mods.AlgorithmClause = ""
					mods.LockClause = ""
				}
			}
		} else {
			log.Debugf("Skipping alter-wrapper for table %s: size=%d < alter-wrapper-min-size=%d", tableName, tableSize, minSize)
		}
	}

	// Get the raw DDL statement as a string.
	ddl.stmt, err = diff.Statement(mods)
	ddl.setErr(err)
	if ddl.stmt == "" {
		// mods may result in a statement that should be skipped, but not due to
		// error. For example, the only change may be to next-auto-inc value, which
		// mods specify to ignore. This is represented by a nil DDLStatement.
		return nil
	}

	// Apply wrapper if relevant
	if wrapper != "" {
		extras := map[string]string{
			"HOST":   ddl.instance.Host,
			"PORT":   strconv.Itoa(ddl.instance.Port),
			"SCHEMA": ddl.schemaName,
			"DDL":    ddl.stmt,
			"TABLE":  tableName,
			"SIZE":   strconv.FormatInt(tableSize, 10),
		}
		if ddl.instance.SocketPath != "" {
			delete(extras, "PORT")
			extras["SOCKET"] = ddl.instance.SocketPath
		}

		switch diff := diff.(type) {
		case tengo.AlterTable:
			prefix := fmt.Sprintf("%s ", diff.Table.AlterStatement())
			extras["CLAUSES"] = strings.Replace(ddl.stmt, prefix, "", 1)
			extras["TYPE"] = "ALTER"
		case tengo.CreateTable:
			prefix := fmt.Sprintf("CREATE TABLE %s ", tengo.EscapeIdentifier(diff.Table.Name))
			extras["CLAUSES"] = strings.Replace(ddl.stmt, prefix, "", 1)
			extras["TYPE"] = "CREATE"
		case tengo.DropTable:
			extras["CLAUSES"] = ""
			extras["TYPE"] = "DROP"
		default: // currently includes case tengo.RenameTable
			ddl.setErr(fmt.Errorf("TableDiff type %T not yet supported", diff))
		}

		ddl.shellOut, err = NewInterpolatedShellOut(wrapper, target.Dir, extras)
		ddl.setErr(err)
	}

	return ddl
}

// IsShellOut returns true if the DDL is to be executed via shelling out to an
// external binary, or false if the DDL represents SQL to be executed directly
// via a standard database connection.
func (ddl *DDLStatement) IsShellOut() bool {
	return (ddl.shellOut != nil)
}

// String returns a string representation of ddl. If an external command is in
// use, the returned string will be prefixed with "\!", the MySQL CLI command
// shortcut for "system" shellout. If ddl.Err is non-nil, the returned string
// will be commented-out by wrapping in /* ... */ long-style comment.
func (ddl *DDLStatement) String() string {
	if ddl == nil {
		return ""
	}
	var stmt string
	if ddl.IsShellOut() {
		stmt = fmt.Sprintf("\\! %s", ddl.shellOut)
	} else {
		stmt = fmt.Sprintf("%s;", ddl.stmt)
	}
	if ddl.Err != nil {
		stmt = fmt.Sprintf("/* %s */", stmt)
	}
	return stmt
}

// Execute runs the DDL statement, either by running a SQL query against a DB,
// or shelling out to an external program, as appropriate.
func (ddl *DDLStatement) Execute() error {
	// Refuse to execute no-ops or errors
	if ddl == nil {
		return nil
	} else if ddl.Err != nil {
		return ddl.Err
	}
	if ddl.IsShellOut() {
		ddl.Err = ddl.shellOut.Run()
	} else {
		if ddl.stmt == "" {
			return errors.New("Attempted to execute empty DDL statement")
		}
		if db, err := ddl.instance.Connect(ddl.schemaName, ""); err != nil {
			ddl.Err = err
		} else {
			_, ddl.Err = db.Exec(ddl.stmt)
		}
	}
	return ddl.Err
}

// setErr sets ddl.Err if the supplied err is non-nil and ddl.Err is nil.
// DDLStatement uses this slightly unusual error convention because errors
// intentionally do not cause an early return in NewDDLStatement; instead they
// just get flagged as erroneous DDL statements, which will never be executed.
func (ddl *DDLStatement) setErr(err error) {
	if err != nil && ddl.Err == nil {
		ddl.Err = err
	}
}

// getTableSize returns the size of the table on the instance corresponding to
// the target. If the table has no rows, this method always returns a size of 0,
// even though information_schema normally indicates at least 16kb in this case.
func (ddl *DDLStatement) getTableSize(target *Target, table *tengo.Table) (int64, error) {
	hasRows, err := target.Instance.TableHasRows(target.SchemaFromInstance, table)
	if !hasRows || err != nil {
		return 0, err
	}
	return target.Instance.TableSize(target.SchemaFromInstance, table)
}
