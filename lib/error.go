package lib

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
)

type WrappedError struct {
	msg      string
	err      error
	location string
}

func (w *WrappedError) Error() string {
	return w.internalError("Error", "")
}

func (w *WrappedError) Unwrap() error {
	return w.err
}

func (w *WrappedError) Is(target error) bool {
	return errors.Is(w.err, target)
}

func (w *WrappedError) internalError(prefix string, indent string) string {
	var sb strings.Builder
	sb.WriteString(indent)
	sb.WriteString(prefix)
	sb.WriteString(" at ")
	sb.WriteString(w.location)
	sb.WriteString(": ")
	sb.WriteString(w.msg)
	if wrapped, ok := w.err.(*WrappedError); ok { //nolint:errorlint
		indent += "  "
		sb.WriteString(wrapped.internalError("\n"+indent+"Cause", indent))
	} else if w.err != nil {
		sb.WriteString("\n" + indent + "Cause: ")
		sb.WriteString(w.err.Error())
	}
	return sb.String()
}

func WrapErrorf(err error, msg string, msgArgs ...any) error {
	return internalWrapErrorf(err, msg, msgArgs...)
}

func internalWrapErrorf(err error, msg string, msgArgs ...any) error {
	pc := make([]uintptr, 3)
	runtime.Callers(3, pc)
	frames := runtime.CallersFrames(pc)
	location := ""
	frame, ok := frames.Next()
	if ok {
		location = fmt.Sprintf("%s:%d", frame.File, frame.Line)
	}
	return &WrappedError{
		msg:      fmt.Sprintf(msg, msgArgs...),
		err:      err,
		location: location,
	}
}

func Errorf(msg string, msgArgs ...any) error {
	return internalWrapErrorf(nil, msg, msgArgs...)
}
