#!/bin/sh -eux

goimports -w .
go tool vet .
! golint . | \
  grep -v "should have comment or be unexported" | \
  grep -v "comment on exported method"
go test ./... $@
