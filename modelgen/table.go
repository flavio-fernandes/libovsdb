package modelgen

import (
	"fmt"
	"sort"
	"strings"
	"text/template"

	"github.com/ovn-org/libovsdb/ovsdb"
)

// NewTableTemplate returns a new table template
// It includes the following other templates that can be overriden to customize the generated file
// "header"
// "preStructDefinitions"
// "structComment"
// "extraFields"
// "extraTags"
// "postStructDefinitions"
// It is design to be used with a map[string] interface and some defined keys (see GetTableTemplateData)
// In addition, the following functions can be used within the template:
// "PrintVal": prints a field value
// "FieldName": prints the name of a field based on its column
// "FieldType": prints the field name based on its column and schema
// "FieldTypeWithEnums": same as FieldType but with enum type expansion
// "OvsdbTag": prints the ovsdb tag
func NewTableTemplate() *template.Template {
	return template.Must(template.New("").Funcs(
		template.FuncMap{
			"PrintVal":           printVal,
			"FieldName":          FieldName,
			"FieldType":          FieldType,
			"FieldTypeWithEnums": FieldTypeWithEnums,
			"OvsdbTag":           Tag,
		},
	).Parse(`
{{- define "header" }}
// Code generated by "libovsdb.modelgen"
// DO NOT EDIT.
{{- end }}
{{ define "preStructDefinitions" }}{{ end }}
{{- define "structComment" }}
// {{ index . "StructName" }} defines an object in {{ index . "TableName" }} table
{{- end }}
{{ define "extraTags" }}{{ end }}
{{ define "extraFields" }}{{ end }}
{{ template "header" . }}
{{ define "postStructDefinitions" }}{{ end }}
{{ define "enums" }}
{{ if index . "WithEnumTypes" }}
{{ if index . "Enums" }}
type (
{{ range index . "Enums" }}
{{ .Alias }} = {{ .Type }}
{{- end }}
)

const (
{{ range  index . "Enums" }}
{{- $e := . }}
{{- range .Sets }}
{{ $e.Alias }}{{ FieldName . }} {{ $e.Alias }} = {{ PrintVal . $e.Type }}
{{- end }}
{{- end }}
)
{{- end }}
{{- end }}
{{- end }}
package {{ index . "PackageName" }}
{{ template "preStructDefinitions" }}
{{ template "enums" . }}
{{ template "structComment" . }}
type {{ index . "StructName" }} struct {
{{- $tableName := index . "TableName" }}
{{ if index . "WithEnumTypes" }}
{{ range $field := index . "Fields" }}	{{ FieldName $field.Column }}  {{ FieldTypeWithEnums $tableName $field.Column $field.Schema }} ` + "`" + `{{ OvsdbTag $field.Column }}{{ template "extraTags" . }}` + "`" + `
{{ end }}
{{ else }}
{{ range  $field := index . "Fields" }}	{{ FieldName $field.Column }}  {{ FieldType $tableName $field.Column $field.Schema }} ` + "`" + `{{ OvsdbTag $field.Column }}{{ template "extraTags" . }}` + "`" + `
{{ end }}
{{ end }}
{{ template "extraFields" . }}
}

{{ template "postStructDefinitions" . }}
`))
}

// Enum represents the enum schema type
type Enum struct {
	Type  string
	Alias string
	Sets  []interface{}
}

// Field represents the field information
type Field struct {
	Column string
	Schema *ovsdb.ColumnSchema
}

// TableTemplateData represents the data used by the Table Template
type TableTemplateData map[string]interface{}

// WithEnumTypes configures whether the Template should expand enum types or not
// Enum expansion (true by default) makes the template define an type alias for each enum type
// and a const for each possible enum value
func (t TableTemplateData) WithEnumTypes(val bool) {
	t["WithEnumTypes"] = val
}

// GetTableTemplateData returns the TableTemplateData map. It has the following keys:
// TableName: (string) the table name
// PackageName : (string) the package name
// StructName: (string) the struct name
// Fields: []Field a list of Fields that the struct has
func GetTableTemplateData(pkg, name string, table *ovsdb.TableSchema) TableTemplateData {
	data := map[string]interface{}{}
	data["TableName"] = name
	data["PackageName"] = pkg
	data["StructName"] = StructName(name)
	Fields := []Field{}
	Enums := []Enum{}

	// Map iteration order is random, so for predictable generation
	// lets sort fields by name
	var order sort.StringSlice
	for columnName := range table.Columns {
		order = append(order, columnName)
	}
	order.Sort()

	for _, columnName := range append([]string{"_uuid"}, order...) {
		columnSchema := table.Column(columnName)
		Fields = append(Fields, Field{
			Column: columnName,
			Schema: columnSchema,
		})
		if enum := FieldEnum(name, columnName, columnSchema); enum != nil {
			Enums = append(Enums, *enum)
		}
	}
	data["Fields"] = Fields
	data["Enums"] = Enums
	data["WithEnumTypes"] = true
	return data
}

// FieldName returns the name of a column field
func FieldName(column string) string {
	return camelCase(strings.Trim(column, "_"))
}

// StructName returns the name of the table struct
func StructName(tableName string) string {
	return strings.Title(strings.ReplaceAll(tableName, "_", ""))
}

func fieldType(tableName, columnName string, column *ovsdb.ColumnSchema, enumTypes bool) string {
	switch column.Type {
	case ovsdb.TypeEnum:
		if enumTypes {
			return enumName(tableName, columnName)
		} else {
			return AtomicType(column.TypeObj.Key.Type)
		}
	case ovsdb.TypeMap:
		return fmt.Sprintf("map[%s]%s", AtomicType(column.TypeObj.Key.Type),
			AtomicType(column.TypeObj.Value.Type))
	case ovsdb.TypeSet:
		if enumTypes && FieldEnum(tableName, columnName, column) != nil {
			return fmt.Sprintf("[]%s", enumName(tableName, columnName))
		}
		return fmt.Sprintf("[]%s", AtomicType(column.TypeObj.Key.Type))
	default:
		return AtomicType(column.Type)
	}
}

// EnumName returns the name of the enum field
func enumName(tableName, columnName string) string {
	return strings.Title(StructName(tableName)) + camelCase(columnName)
}

// FieldType returns the string representation of a column type without enum types expansion
func FieldType(tableName, columnName string, column *ovsdb.ColumnSchema) string {
	return fieldType(tableName, columnName, column, false)
}

// FieldTypeWithEnums returns the string representation of a column type where Enums
// are expanded into their own types
func FieldTypeWithEnums(tableName, columnName string, column *ovsdb.ColumnSchema) string {
	return fieldType(tableName, columnName, column, true)
}

// FieldEnum returns the Enum if the column is an enum type
func FieldEnum(tableName, columnName string, column *ovsdb.ColumnSchema) *Enum {
	if column.TypeObj == nil || column.TypeObj.Key.Enum == nil {
		return nil
	}
	return &Enum{
		Type:  column.TypeObj.Key.Type,
		Alias: enumName(tableName, columnName),
		Sets:  column.TypeObj.Key.Enum,
	}
}

// BasicType returns the string type of an AtomicType
func AtomicType(atype string) string {
	switch atype {
	case ovsdb.TypeInteger:
		return "int"
	case ovsdb.TypeReal:
		return "float64"
	case ovsdb.TypeBoolean:
		return "bool"
	case ovsdb.TypeString:
		return "string"
	case ovsdb.TypeUUID:
		return "string"
	}
	return ""
}

// Tag returns the Tag string of a column
func Tag(column string) string {
	return fmt.Sprintf("ovsdb:\"%s\"", column)
}

// Filename returns the filename of a table
func FileName(table string) string {
	return fmt.Sprintf("%s.go", strings.ToLower(table))
}

// common initialisms used in ovsdb schemas
var initialisms = map[string]bool{
	"ACL":   true,
	"BFD":   true,
	"CFM":   true,
	"CT":    true,
	"CVLAN": true,
	"DNS":   true,
	"DSCP":  true,
	"ID":    true,
	"IP":    true,
	"IPFIX": true,
	"LACP":  true,
	"LLDP":  true,
	"MAC":   true,
	"MTU":   true,
	"OVS":   true,
	"QOS":   true,
	"RSTP":  true,
	"SSL":   true,
	"STP":   true,
	"TCP":   true,
	"SCTP":  true,
	"UDP":   true,
	"UUID":  true,
	"VLAN":  true,
	"STT":   true,
	"DNAT":  true,
	"SNAT":  true,
	"ICMP":  true,
	"SLB":   true,
}

func camelCase(field string) string {
	s := strings.ToLower(field)
	parts := strings.FieldsFunc(s, func(r rune) bool {
		return r == '_' || r == '-'
	})
	if len(parts) > 1 {
		s = ""
		for _, p := range parts {
			s += strings.Title(expandInitilaisms(p))
		}
	} else {
		s = strings.Title(expandInitilaisms(s))
	}
	return s
}

func expandInitilaisms(s string) string {
	// check initialisms
	if u := strings.ToUpper(s); initialisms[u] {
		return strings.ToUpper(s)
	}
	// check for plurals too
	if strings.HasSuffix(s, "s") {
		sub := s[:len(s)-1]
		if u := strings.ToUpper(sub); initialisms[u] {
			return strings.ToUpper(sub) + "s"
		}
	}
	return s
}

func printVal(v interface{}, t string) string {
	switch t {
	case "int":
		return fmt.Sprintf(`%d`, v)
	case "float64":
		return fmt.Sprintf(`%f`, v)
	case "bool":
		return fmt.Sprintf(`%t`, v)
	case "string":
		return fmt.Sprintf(`"%s"`, v)
	}
	return ""
}
