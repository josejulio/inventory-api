package schema

import "context"

type ValidationSchema interface {
	Validate(data interface{}) (bool, error)
}

type ValidationService interface {
	IsReporterForResource(ctx context.Context, resourceType string, reporterType string) (bool, error)
	CommonShallowValidate(ctx context.Context, resourceType string, commonRepresentation map[string]interface{}) error
	ReporterShallowValidate(ctx context.Context, resourceType string, reporterType string, reporterRepresentation map[string]interface{}) error
}
