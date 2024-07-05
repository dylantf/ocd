#!/bin/bash

# Activate the project switch
eval $(opam env)

# Install dependencies
opam install . --deps-only --with-test

# Build the project
dune build