package template

import (
	"bytes"
	"context"
	"encoding/xml"
	"strings"
)

const (
	TypeText       = "text"
	TypeNotEmpty   = "notEmpty"
	TypeEmpty      = "empty"
	TypeEqual      = "equal"
	TypeNotEqual   = "notEqual"
	ParamText      = "text"
	ParamParameter = "param"
)

type StringFormat struct {
	Texts      []string    `mapstructure:"texts" json:"texts,omitempty" gorm:"column:texts" bson:"texts,omitempty" dynamodbav:"texts,omitempty" firestore:"texts,omitempty"`
	Parameters []Parameter `mapstructure:"parameters" json:"parameters,omitempty" gorm:"column:parameters" bson:"parameters,omitempty" dynamodbav:"parameters,omitempty" firestore:"parameters,omitempty"`
}
type Parameter struct {
	Name string `mapstructure:"name" json:"name,omitempty" gorm:"column:name" bson:"name,omitempty" dynamodbav:"name,omitempty" firestore:"name,omitempty"`
	Type string `mapstructure:"type" json:"type,omitempty" gorm:"column:type" bson:"type,omitempty" dynamodbav:"type,omitempty" firestore:"type,omitempty"`
}
type TemplateNode struct {
	Type   string       `mapstructure:"type" json:"type,omitempty" gorm:"column:type" bson:"type,omitempty" dynamodbav:"type,omitempty" firestore:"type,omitempty"`
	Text   string       `mapstructure:"text" json:"text,omitempty" gorm:"column:text" bson:"text,omitempty" dynamodbav:"text,omitempty" firestore:"text,omitempty"`
	Name   string       `mapstructure:"name" json:"name,omitempty" gorm:"column:name" bson:"name,omitempty" dynamodbav:"name,omitempty" firestore:"name,omitempty"`
	Encode string       `mapstructure:"encode" json:"encode,omitempty" gorm:"column:encode" bson:"encode,omitempty" dynamodbav:"encode,omitempty" firestore:"encode,omitempty"`
	Value  string       `mapstructure:"value" json:"value,omitempty" gorm:"column:value" bson:"value,omitempty" dynamodbav:"value,omitempty" firestore:"value,omitempty"`
	Format StringFormat `mapstructure:"format" json:"format,omitempty" gorm:"column:format" bson:"format,omitempty" dynamodbav:"format,omitempty" firestore:"format,omitempty"`
}
type Template struct {
	Text      string         `mapstructure:"text" json:"text,omitempty" gorm:"column:text" bson:"text,omitempty" dynamodbav:"text,omitempty" firestore:"text,omitempty"`
	Templates []TemplateNode `mapstructure:"templates" json:"templates,omitempty" gorm:"column:templates" bson:"templates,omitempty" dynamodbav:"templates,omitempty" firestore:"templates,omitempty"`
}

type TemplateBuilder interface {
	Build(ctx context.Context, stream string) (*Template, error)
}
type XmlTemplateBuilder struct {
}

func NewXmlTemplateBuilder() *XmlTemplateBuilder {
	return &XmlTemplateBuilder{}
}
func (b *XmlTemplateBuilder) Build(ctx context.Context, stream string) (*Template, error) {
	return BuildTemplate(ctx, stream)
}
func BuildTemplate(ctx context.Context, stream string) (*Template, error) {
	data := []byte(stream)
	buf := bytes.NewBuffer(data)
	dec := xml.NewDecoder(buf)
	ns := make([]TemplateNode, 0)
	texts := make([]string, 0)
	for {
		token, er0 := dec.Token()
		if token == nil {
			break
		}
		if er0 != nil {
			return nil, er0
		}
		switch element := token.(type) {
		case xml.CharData:
			s := string([]byte(element))
			if s != "\n" {
				n := TemplateNode{Type: "text", Text: s}
				texts = append(texts, s)
				n.Format = BuildFormat(n.Text)
				ns = append(ns, n)
			}
		case xml.StartElement:
			if element.Name.Local == "notEmpty" {
				encode := GetValue(element.Attr, "encode")
				n := TemplateNode{Type: "notEmpty", Encode: encode}
				sub, er1 := dec.Token()
				if er1 != nil {
					return nil, er1
				}
				switch inner := sub.(type) {
				case xml.CharData:
					s2 := string([]byte(inner))
					n.Text = s2
					n.Format = BuildFormat(n.Text)
					texts = append(texts, s2)
				}
				ns = append(ns, n)
			} else if element.Name.Local == "empty" {
				encode := GetValue(element.Attr, "encode")
				n := TemplateNode{Type: "empty", Encode: encode}
				sub, er1 := dec.Token()
				if er1 != nil {
					return nil, er1
				}
				switch inner := sub.(type) {
				case xml.CharData:
					s2 := string([]byte(inner))
					n.Text = s2
					n.Format = BuildFormat(n.Text)
					texts = append(texts, s2)
				}
				ns = append(ns, n)
			} else if element.Name.Local == "equal" {
				encode := GetValue(element.Attr, "encode")
				v := GetValue(element.Attr, "value")
				n := TemplateNode{Type: "equal", Encode: encode, Value: v}
				sub, er1 := dec.Token()
				if er1 != nil {
					return nil, er1
				}
				switch inner := sub.(type) {
				case xml.CharData:
					s2 := string([]byte(inner))
					n.Text = s2
					n.Format = BuildFormat(n.Text)
					texts = append(texts, s2)
				}
				ns = append(ns, n)
			} else if element.Name.Local == "notEqual" {
				encode := GetValue(element.Attr, "encode")
				v := GetValue(element.Attr, "value")
				n := TemplateNode{Type: "notEqual", Encode: encode, Value: v}
				sub, er1 := dec.Token()
				if er1 != nil {
					return nil, er1
				}
				switch inner := sub.(type) {
				case xml.CharData:
					s2 := string([]byte(inner))
					n.Text = s2
					n.Format = BuildFormat(n.Text)
					texts = append(texts, s2)
				}
				ns = append(ns, n)
			}
		}
	}
	t := Template{}
	t.Text = strings.Join(texts, " ")
	t.Templates = ns
	return &t, nil
}
func GetValue(attrs []xml.Attr, name string) string {
	if len(attrs) <= 0 {
		return ""
	}
	for _, attr := range attrs {
		if attr.Name.Local == name {
			return attr.Value
		}
	}
	return ""
}

func BuildFormat(str string) StringFormat {
	str2 := str
	str2b := str
	var str3 string
	texts := make([]string, 0)
	parameters := make([]Parameter, 0)
	var from, i, j int
	for {
		i = strings.Index(str2b, "{")
		if i >= 0 {
			str3 = str2b[i+1:]
			j = strings.Index(str3, "}")
			if j >= 0 {
				pro := str2b[i+1 : i+j+1]
				if IsValidProperty(pro) {
					p := Parameter{}
					p.Name = pro
					if i >= 1 {
						var chr = string(str2b[i-1])
						if chr == "#" {
							texts = append(texts, str2[:from+i-1])
							p.Type = "param"
						} else if chr == "$" {
							texts = append(texts, str2[:from+i-1])
							p.Type = "text"
						} else {
							texts = append(texts, str2[:from+i])
							p.Type = "text"
						}
					} else {
						texts = append(texts, str2[:from+i])
						p.Type = "text"
					}
					parameters = append(parameters, p)
					from = from + i + j + 2
					str2 = str2[from:]
					str2b = str2
					from = 0
				} else {
					from = i + 1
					str2b = str2[i+1:]
				}
			} else {
				from = i + 1
				str2b = str2[from:]
			}
		} else {
			texts = append(texts, str2)
			break
		}
	}
	f := StringFormat{}
	f.Texts = texts
	f.Parameters = parameters
	return f
}

func IsValidProperty(v string) bool {
	var len = len(v) - 1
	for i := 0; i <= len; i++ {
		var chr = string(v[i])
		if !((chr >= "0" && chr <= "9") || (chr >= "A" && chr <= "Z") || (chr >= "a" && chr <= "z") || chr == "_" || chr == ".") {
			return false
		}
	}
	return true
}
