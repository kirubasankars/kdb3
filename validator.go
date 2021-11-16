package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/qri-io/jsonschema"
	"github.com/valyala/fastjson"
)

type SchemaValidator interface {
	Setup(designDocs []Document)
	Validate(doc *Document) []string
	ValidateSchema(doc Document) error
}

type DefaultJSONSchemaValidator struct {
	DBName string
	ctx    context.Context
	schema map[string]*jsonschema.Schema
}

func (validator *DefaultJSONSchemaValidator) Setup(designDocs []Document) {
	validator.ctx = context.Background()
	validator.schema = make(map[string]*jsonschema.Schema)

	var designDoc Document
	for _, x := range designDocs {
		if x.ID == "_design/_schema" {
			designDoc = x
			break
		}
	}

	if designDoc.ID == "" {
		return
	}

	doc, _ := ParseDocument(designDoc.Data)

	designDocValidator := &DesignDocumentValidator{}
	designDocValidator.Hash = doc.Hash
	designDocValidator.Version = doc.Version

	jsonValue, _ := parserPool.Get().ParseBytes(doc.Data)

	schemaObject := jsonValue.GetObject("schema")
	if schemaObject != nil {
		schemaObject.Visit(func(key []byte, value *fastjson.Value) {
			rs := &jsonschema.Schema{}
			value.Set("type", fastjson.MustParse("\"object\""))
			json.Unmarshal([]byte(value.String()), rs)
			validator.schema[string(key)] = rs
		})
	}
}

func (validator *DefaultJSONSchemaValidator) Validate(doc *Document) []string {
	if doc.Kind == "" {
		return nil
	}

	if schema, ok := validator.schema[doc.Kind]; ok {
		if errs, _ := schema.ValidateBytes(validator.ctx, doc.Data); len(errs) > 0 {
			var errors []string
			for _, v := range errs {
				errors = append(errors, v.Message)
			}
			return errors
		}
	}

	return nil
}

func (validator *DefaultJSONSchemaValidator) ValidateSchema(doc Document) error {
	jsonValue, _ := parserPool.Get().ParseBytes(doc.Data)

	schemaObject := jsonValue.GetObject("schema")
	if schemaObject != nil {
		var err error
		schemaObject.Visit(func(key []byte, value *fastjson.Value) {
			rs := &jsonschema.Schema{}
			if err = json.Unmarshal([]byte(value.String()), rs); err != nil {
				err = fmt.Errorf("%s: %w", err, ErrInternalError)
			}
		})
		return err
	}
	return nil
}
