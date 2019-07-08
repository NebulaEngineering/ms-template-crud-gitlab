#!/bin/bash

# FrontEnd - EMI composition
nebulae compose-ui development --shell-type=FUSE2_ANGULAR --shell-repo=https://gitlab.com/git_repo_directory_path/frontendid --frontend-id=frontendid --output-dir=frontendid  --setup-file=../etc/mfe-setup.json

# API - GateWay composition
nebulae compose-api development --api-type=NEBULAE_GATEWAY --api-repo=https://gitlab.com/git_repo_directory_path/apiid --api-id=apiid --output-dir=apiid  --setup-file=../etc/mapi-setup.json
