package factory

import (
	"context"
	"fmt"

	schema2 "github.com/project-kessel/inventory-api/cmd/config/schema"
	"github.com/project-kessel/inventory-api/cmd/config/schema/inmemory"
	"github.com/project-kessel/inventory-api/internal/biz/schema"
	"github.com/project-kessel/inventory-api/internal/data"

	"github.com/go-kratos/kratos/v2/log"
)

func NewSchemaService(ctx context.Context, c schema2.CompletedConfig, logger *log.Helper) (*data.SchemaService, error) {
	repository, err := newSchemaRepository(ctx, c, logger)
	if err != nil {
		return nil, err
	}

	return data.NewSchemaService(repository), nil
}

func newSchemaRepository(ctx context.Context, c schema2.CompletedConfig, logger *log.Helper) (schema.Repository, error) {
	switch c.Repository {
	case schema2.InMemoryRepository:
		switch c.InMemory.Type {
		case inmemory.EmptyRepository:
			return data.NewInMemorySchemaRepository(), nil
		case inmemory.JSONRepository:
			return data.NewInMemorySchemaRepositoryFromJsonFile(ctx, c.InMemory.Path, data.NewJsonSchemaValidatorFromString)
		case inmemory.DirRepository:
			return data.NewInMemorySchemaRepositoryFromDir(ctx, c.InMemory.Path, data.NewJsonSchemaValidatorFromString)
		}
	}

	return nil, fmt.Errorf("invalid repository type: %s", c.Repository)
}
