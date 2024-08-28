// Code generated by protoc-gen-validate. DO NOT EDIT.
// source: kessel/inventory/v1beta1/k8s_policies_service.proto

package v1beta1

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/mail"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/types/known/anypb"
)

// ensure the imports are used
var (
	_ = bytes.MinRead
	_ = errors.New("")
	_ = fmt.Print
	_ = utf8.UTFMax
	_ = (*regexp.Regexp)(nil)
	_ = (*strings.Reader)(nil)
	_ = net.IPv4len
	_ = time.Duration(0)
	_ = (*url.URL)(nil)
	_ = (*mail.Address)(nil)
	_ = anypb.Any{}
	_ = sort.Sort
)

// Validate checks the field values on CreateK8SPolicyRequest with the rules
// defined in the proto definition for this message. If any rules are
// violated, the first error encountered is returned, or nil if there are no violations.
func (m *CreateK8SPolicyRequest) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on CreateK8SPolicyRequest with the rules
// defined in the proto definition for this message. If any rules are
// violated, the result is a list of violation errors wrapped in
// CreateK8SPolicyRequestMultiError, or nil if none found.
func (m *CreateK8SPolicyRequest) ValidateAll() error {
	return m.validate(true)
}

func (m *CreateK8SPolicyRequest) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	if all {
		switch v := interface{}(m.GetK8SPolicy()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, CreateK8SPolicyRequestValidationError{
					field:  "K8SPolicy",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, CreateK8SPolicyRequestValidationError{
					field:  "K8SPolicy",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetK8SPolicy()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return CreateK8SPolicyRequestValidationError{
				field:  "K8SPolicy",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if len(errors) > 0 {
		return CreateK8SPolicyRequestMultiError(errors)
	}

	return nil
}

// CreateK8SPolicyRequestMultiError is an error wrapping multiple validation
// errors returned by CreateK8SPolicyRequest.ValidateAll() if the designated
// constraints aren't met.
type CreateK8SPolicyRequestMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m CreateK8SPolicyRequestMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m CreateK8SPolicyRequestMultiError) AllErrors() []error { return m }

// CreateK8SPolicyRequestValidationError is the validation error returned by
// CreateK8SPolicyRequest.Validate if the designated constraints aren't met.
type CreateK8SPolicyRequestValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e CreateK8SPolicyRequestValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e CreateK8SPolicyRequestValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e CreateK8SPolicyRequestValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e CreateK8SPolicyRequestValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e CreateK8SPolicyRequestValidationError) ErrorName() string {
	return "CreateK8SPolicyRequestValidationError"
}

// Error satisfies the builtin error interface
func (e CreateK8SPolicyRequestValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sCreateK8SPolicyRequest.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = CreateK8SPolicyRequestValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = CreateK8SPolicyRequestValidationError{}

// Validate checks the field values on CreateK8SPolicyResponse with the rules
// defined in the proto definition for this message. If any rules are
// violated, the first error encountered is returned, or nil if there are no violations.
func (m *CreateK8SPolicyResponse) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on CreateK8SPolicyResponse with the
// rules defined in the proto definition for this message. If any rules are
// violated, the result is a list of violation errors wrapped in
// CreateK8SPolicyResponseMultiError, or nil if none found.
func (m *CreateK8SPolicyResponse) ValidateAll() error {
	return m.validate(true)
}

func (m *CreateK8SPolicyResponse) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	if all {
		switch v := interface{}(m.GetK8SPolicy()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, CreateK8SPolicyResponseValidationError{
					field:  "K8SPolicy",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, CreateK8SPolicyResponseValidationError{
					field:  "K8SPolicy",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetK8SPolicy()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return CreateK8SPolicyResponseValidationError{
				field:  "K8SPolicy",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if len(errors) > 0 {
		return CreateK8SPolicyResponseMultiError(errors)
	}

	return nil
}

// CreateK8SPolicyResponseMultiError is an error wrapping multiple validation
// errors returned by CreateK8SPolicyResponse.ValidateAll() if the designated
// constraints aren't met.
type CreateK8SPolicyResponseMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m CreateK8SPolicyResponseMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m CreateK8SPolicyResponseMultiError) AllErrors() []error { return m }

// CreateK8SPolicyResponseValidationError is the validation error returned by
// CreateK8SPolicyResponse.Validate if the designated constraints aren't met.
type CreateK8SPolicyResponseValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e CreateK8SPolicyResponseValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e CreateK8SPolicyResponseValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e CreateK8SPolicyResponseValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e CreateK8SPolicyResponseValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e CreateK8SPolicyResponseValidationError) ErrorName() string {
	return "CreateK8SPolicyResponseValidationError"
}

// Error satisfies the builtin error interface
func (e CreateK8SPolicyResponseValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sCreateK8SPolicyResponse.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = CreateK8SPolicyResponseValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = CreateK8SPolicyResponseValidationError{}

// Validate checks the field values on UpdateK8SPolicyRequest with the rules
// defined in the proto definition for this message. If any rules are
// violated, the first error encountered is returned, or nil if there are no violations.
func (m *UpdateK8SPolicyRequest) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on UpdateK8SPolicyRequest with the rules
// defined in the proto definition for this message. If any rules are
// violated, the result is a list of violation errors wrapped in
// UpdateK8SPolicyRequestMultiError, or nil if none found.
func (m *UpdateK8SPolicyRequest) ValidateAll() error {
	return m.validate(true)
}

func (m *UpdateK8SPolicyRequest) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	// no validation rules for Resource

	if all {
		switch v := interface{}(m.GetK8SPolicy()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, UpdateK8SPolicyRequestValidationError{
					field:  "K8SPolicy",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, UpdateK8SPolicyRequestValidationError{
					field:  "K8SPolicy",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetK8SPolicy()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return UpdateK8SPolicyRequestValidationError{
				field:  "K8SPolicy",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if len(errors) > 0 {
		return UpdateK8SPolicyRequestMultiError(errors)
	}

	return nil
}

// UpdateK8SPolicyRequestMultiError is an error wrapping multiple validation
// errors returned by UpdateK8SPolicyRequest.ValidateAll() if the designated
// constraints aren't met.
type UpdateK8SPolicyRequestMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m UpdateK8SPolicyRequestMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m UpdateK8SPolicyRequestMultiError) AllErrors() []error { return m }

// UpdateK8SPolicyRequestValidationError is the validation error returned by
// UpdateK8SPolicyRequest.Validate if the designated constraints aren't met.
type UpdateK8SPolicyRequestValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e UpdateK8SPolicyRequestValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e UpdateK8SPolicyRequestValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e UpdateK8SPolicyRequestValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e UpdateK8SPolicyRequestValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e UpdateK8SPolicyRequestValidationError) ErrorName() string {
	return "UpdateK8SPolicyRequestValidationError"
}

// Error satisfies the builtin error interface
func (e UpdateK8SPolicyRequestValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sUpdateK8SPolicyRequest.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = UpdateK8SPolicyRequestValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = UpdateK8SPolicyRequestValidationError{}

// Validate checks the field values on UpdateK8SPolicyResponse with the rules
// defined in the proto definition for this message. If any rules are
// violated, the first error encountered is returned, or nil if there are no violations.
func (m *UpdateK8SPolicyResponse) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on UpdateK8SPolicyResponse with the
// rules defined in the proto definition for this message. If any rules are
// violated, the result is a list of violation errors wrapped in
// UpdateK8SPolicyResponseMultiError, or nil if none found.
func (m *UpdateK8SPolicyResponse) ValidateAll() error {
	return m.validate(true)
}

func (m *UpdateK8SPolicyResponse) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	if len(errors) > 0 {
		return UpdateK8SPolicyResponseMultiError(errors)
	}

	return nil
}

// UpdateK8SPolicyResponseMultiError is an error wrapping multiple validation
// errors returned by UpdateK8SPolicyResponse.ValidateAll() if the designated
// constraints aren't met.
type UpdateK8SPolicyResponseMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m UpdateK8SPolicyResponseMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m UpdateK8SPolicyResponseMultiError) AllErrors() []error { return m }

// UpdateK8SPolicyResponseValidationError is the validation error returned by
// UpdateK8SPolicyResponse.Validate if the designated constraints aren't met.
type UpdateK8SPolicyResponseValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e UpdateK8SPolicyResponseValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e UpdateK8SPolicyResponseValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e UpdateK8SPolicyResponseValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e UpdateK8SPolicyResponseValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e UpdateK8SPolicyResponseValidationError) ErrorName() string {
	return "UpdateK8SPolicyResponseValidationError"
}

// Error satisfies the builtin error interface
func (e UpdateK8SPolicyResponseValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sUpdateK8SPolicyResponse.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = UpdateK8SPolicyResponseValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = UpdateK8SPolicyResponseValidationError{}

// Validate checks the field values on DeleteK8SPolicyRequest with the rules
// defined in the proto definition for this message. If any rules are
// violated, the first error encountered is returned, or nil if there are no violations.
func (m *DeleteK8SPolicyRequest) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on DeleteK8SPolicyRequest with the rules
// defined in the proto definition for this message. If any rules are
// violated, the result is a list of violation errors wrapped in
// DeleteK8SPolicyRequestMultiError, or nil if none found.
func (m *DeleteK8SPolicyRequest) ValidateAll() error {
	return m.validate(true)
}

func (m *DeleteK8SPolicyRequest) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	// no validation rules for Resource

	if len(errors) > 0 {
		return DeleteK8SPolicyRequestMultiError(errors)
	}

	return nil
}

// DeleteK8SPolicyRequestMultiError is an error wrapping multiple validation
// errors returned by DeleteK8SPolicyRequest.ValidateAll() if the designated
// constraints aren't met.
type DeleteK8SPolicyRequestMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m DeleteK8SPolicyRequestMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m DeleteK8SPolicyRequestMultiError) AllErrors() []error { return m }

// DeleteK8SPolicyRequestValidationError is the validation error returned by
// DeleteK8SPolicyRequest.Validate if the designated constraints aren't met.
type DeleteK8SPolicyRequestValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e DeleteK8SPolicyRequestValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e DeleteK8SPolicyRequestValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e DeleteK8SPolicyRequestValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e DeleteK8SPolicyRequestValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e DeleteK8SPolicyRequestValidationError) ErrorName() string {
	return "DeleteK8SPolicyRequestValidationError"
}

// Error satisfies the builtin error interface
func (e DeleteK8SPolicyRequestValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sDeleteK8SPolicyRequest.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = DeleteK8SPolicyRequestValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = DeleteK8SPolicyRequestValidationError{}

// Validate checks the field values on DeleteK8SPolicyResponse with the rules
// defined in the proto definition for this message. If any rules are
// violated, the first error encountered is returned, or nil if there are no violations.
func (m *DeleteK8SPolicyResponse) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on DeleteK8SPolicyResponse with the
// rules defined in the proto definition for this message. If any rules are
// violated, the result is a list of violation errors wrapped in
// DeleteK8SPolicyResponseMultiError, or nil if none found.
func (m *DeleteK8SPolicyResponse) ValidateAll() error {
	return m.validate(true)
}

func (m *DeleteK8SPolicyResponse) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	if len(errors) > 0 {
		return DeleteK8SPolicyResponseMultiError(errors)
	}

	return nil
}

// DeleteK8SPolicyResponseMultiError is an error wrapping multiple validation
// errors returned by DeleteK8SPolicyResponse.ValidateAll() if the designated
// constraints aren't met.
type DeleteK8SPolicyResponseMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m DeleteK8SPolicyResponseMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m DeleteK8SPolicyResponseMultiError) AllErrors() []error { return m }

// DeleteK8SPolicyResponseValidationError is the validation error returned by
// DeleteK8SPolicyResponse.Validate if the designated constraints aren't met.
type DeleteK8SPolicyResponseValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e DeleteK8SPolicyResponseValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e DeleteK8SPolicyResponseValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e DeleteK8SPolicyResponseValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e DeleteK8SPolicyResponseValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e DeleteK8SPolicyResponseValidationError) ErrorName() string {
	return "DeleteK8SPolicyResponseValidationError"
}

// Error satisfies the builtin error interface
func (e DeleteK8SPolicyResponseValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sDeleteK8SPolicyResponse.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = DeleteK8SPolicyResponseValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = DeleteK8SPolicyResponseValidationError{}
