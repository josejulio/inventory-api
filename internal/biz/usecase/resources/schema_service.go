package resources

import (
	"context"
	"errors"
	"fmt"

	"github.com/project-kessel/inventory-api/internal/biz/schema"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/project-kessel/inventory-api/internal/biz"
	"github.com/project-kessel/inventory-api/internal/biz/model"
	"github.com/project-kessel/inventory-api/internal/data"
)

type SchemaUsecase struct {
	resourceRepository data.ResourceRepository
	schemaRepository   schema.Repository
	Log                *log.Helper
}

func NewSchemaUsecase(resourceRepository data.ResourceRepository, schemaRepository schema.Repository, logger *log.Helper) *SchemaUsecase {
	return &SchemaUsecase{
		resourceRepository: resourceRepository,
		schemaRepository:   schemaRepository,
		Log:                logger,
	}
}

// CalculateTuples : For now we are determining ReportTupleEvent vs DeleteTupleEvent based on Operation Type, but the eventual goal is for input to be ReportResourceEvent and DeleteResourceEvent
func (sc *SchemaUsecase) CalculateTuples(tupleEvent model.TupleEvent, operationType biz.EventOperationType) (model.TuplesToReplicate, error) {

	sc.Log.Infof("Calculating Tuples for operationType and event: %d, key: %+v", operationType, tupleEvent)

	switch operationType.OperationType() {
	case biz.OperationTypeDeleted:
		return sc.processDeleteTupleEvent(tupleEvent)
	default:
		return sc.processReportTupleEvent(tupleEvent, operationType)
	}
}

// IsReporterForResource validates the resourceType and reporterType combination is valid. i.e. that there is a reporter that reports said resource.
func (sc *SchemaUsecase) IsReporterForResource(ctx context.Context, resourceType string, reporterType string) (bool, error) {
	if _, err := sc.schemaRepository.GetReporterSchema(ctx, resourceType, reporterType); err != nil {
		if errors.Is(err, schema.ResourceSchemaNotFound) || errors.Is(err, schema.ReporterSchemaNotfound) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// CommonShallowValidate validates the common representation for a given resourceType.
func (sc *SchemaUsecase) CommonShallowValidate(ctx context.Context, resourceType string, commonRepresentation map[string]interface{}) error {
	resource, err := sc.schemaRepository.GetResourceSchema(ctx, resourceType)
	if err != nil {
		return fmt.Errorf("failed to load common representation schema for '%s': %w", resourceType, err)
	}

	if resource.ValidationSchema == nil {
		return fmt.Errorf("no schema found for '%s'", resourceType)
	}

	hasCommonRepresentationData := len(commonRepresentation) > 0
	if !hasCommonRepresentationData {
		commonRepresentation = map[string]interface{}{}
	}

	_, err = resource.ValidationSchema.Validate(commonRepresentation)
	if err != nil {
		if hasCommonRepresentationData {
			return err
		}
		return fmt.Errorf("missing 'common' field in payload - schema for '%s' has required fields: %w", resourceType, err)
	}

	return nil
}

// ReporterShallowValidate validates the specific reporter representation for a given resourceType/reporterType.
func (sc *SchemaUsecase) ReporterShallowValidate(ctx context.Context, resourceType string, reporterType string, reporterRepresentation map[string]interface{}) error {
	reporter, err := sc.schemaRepository.GetReporterSchema(ctx, resourceType, reporterType)
	if err != nil {
		return err
	}

	// Case 1: No schema found for resourceType:reporterType
	if reporter.ValidationSchema == nil {
		if len(reporterRepresentation) > 0 {
			return fmt.Errorf("no schema found for '%s:%s', but reporter representation was provided. Submission is not allowed", resourceType, reporterType)
		}
		log.Debugf("no schema found for %s:%s, treating as abstract reporter representation", resourceType, reporterType)
		return nil
	}

	hasReporterRepresentationData := len(reporterRepresentation) > 0
	if !hasReporterRepresentationData {
		reporterRepresentation = map[string]interface{}{}
	}

	_, err = reporter.ValidationSchema.Validate(reporterRepresentation)
	if err != nil {
		if hasReporterRepresentationData {
			return err
		}

		// If schema has validation errors but reporterRepresentation is nil/empty, that's an error
		return fmt.Errorf("missing 'reporter' field in payload - schema for '%s:%s' has required fields: %w", resourceType, reporterType, err)
	}

	return nil
}

func (sc *SchemaUsecase) processReportTupleEvent(tupleEvent model.TupleEvent, operationType biz.EventOperationType) (model.TuplesToReplicate, error) {
	key := tupleEvent.ReporterResourceKey()

	if tupleEvent.CommonVersion() == nil {
		return model.TuplesToReplicate{}, nil
	}

	version := tupleEvent.CommonVersion().Uint()
	currentVersion := &version

	representations, err := sc.resourceRepository.FindCurrentAndPreviousVersionedRepresentations(
		nil, key, currentVersion, operationType,
	)
	if err != nil {
		return model.TuplesToReplicate{}, fmt.Errorf("failed to find representations: %w", err)
	}

	currentWorkspaceID, previousWorkspaceID := data.GetCurrentAndPreviousWorkspaceID(representations, version)
	return sc.buildTuplesToReplicate(currentWorkspaceID, previousWorkspaceID, key)
}

func (sc *SchemaUsecase) processDeleteTupleEvent(tupleEvent model.TupleEvent) (model.TuplesToReplicate, error) {
	key := tupleEvent.ReporterResourceKey()

	representation, err := sc.resourceRepository.FindLatestRepresentations(nil, key)
	if err != nil {
		return model.TuplesToReplicate{}, fmt.Errorf("failed to find representations: %w", err)
	}

	currentWorkspaceID := data.ExtractWorkspaceID(representation)
	return sc.buildTuplesToReplicate("", currentWorkspaceID, key)
}

func (sc *SchemaUsecase) buildTuplesToReplicate(currentWorkspaceID, previousWorkspaceID string, key model.ReporterResourceKey) (model.TuplesToReplicate, error) {
	if previousWorkspaceID != "" && previousWorkspaceID == currentWorkspaceID {
		return model.TuplesToReplicate{}, nil
	}

	var tuplesToCreate, tuplesToDelete []model.RelationsTuple

	if currentWorkspaceID != "" {
		tuplesToCreate = append(tuplesToCreate, model.NewWorkspaceRelationsTuple(currentWorkspaceID, key))
	}

	if previousWorkspaceID != "" {
		tuplesToDelete = append(tuplesToDelete, model.NewWorkspaceRelationsTuple(previousWorkspaceID, key))
	}

	return model.NewTuplesToReplicate(tuplesToCreate, tuplesToDelete)
}
