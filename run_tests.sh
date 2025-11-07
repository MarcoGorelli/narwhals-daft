#!/bin/bash

TESTS_THAT_NEED_FIX=" \
test_truncate or \
test_namespace_len or \
test_to_uppercase \
"

pytest narwhals/tests \
  -p pytest_constructor_override \
  -p env \
  --use-external-constructor \
  --constructors daft \
  -k "not ( \
      ${TESTS_THAT_NEED_FIX} \
  )" \
  "$@"
