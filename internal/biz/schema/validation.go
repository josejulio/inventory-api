package schema

type ValidationSchema interface {
	Validate(data interface{}) (bool, error)
}
