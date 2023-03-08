#!/usr/bin/env bash
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

set -x

typeset -i RETSTAT=0
DEFAULT_PROFILE="default"
DEFAULT_REGION=$(aws configure get region --profile ${DEFAULT_PROFILE})
BASENAME=$(basename -a $0)
DIRNAME=$(pwd)
DEFAULT_REPO=""

usage() {
    echo "Usage Options for ${BASENAME}
    -s : Name of the AWS Profile for the CICD Account
    -t : Name of the Child CICD account
    -r : Region for the Deployment
    -d : Name of Repositories to Delete
    -h : Displays this help message
    "
}

delete_repositories() {

    for REPOSITORY in ${REPOSITORIES[@]}; do
        REPOSITORY_DETAILS=$(aws codecommit get-repository --repository-name ${REPOSITORY} --region ${REGION} --profile ${CICD_PROFILE} &>/dev/null)
        RETSTAT=$?
        echo "Deleting REPO ${REPOSITORY}"
        if [ ${RETSTAT} -eq 0 ]; then
            echo "Remote Repository ${REPOSITORY} already exists, deleting..."
            aws codecommit delete-repository --profile ${CICD_PROFILE} --region ${REGION} --repository-name ${REPOSITORY}
            # pushd ${REPOSITORY}
            rm -rf .git
            # popd
        else
            echo "Repository ${REPOSITORY} does not exists in ${REGION}"
            echo "Deleting local .git directory if it exists."
            # pushd ${REPOSITORY}
            if [ -d .git ]; then
                echo "Local .git Repository exists, deleting..."
                rm -rf .git
            else
                echo "Local .git Repository does not exist."
            fi
            # popd
        fi
    done

}

while getopts "s:t:r:d:h" option; do
    case ${option} in
    s) CICD_PROFILE=${OPTARG:-$DEFAULT_PROFILE} ;;
    t) CHILD_PROFILE=${OPTARG:-$DEFAULT_PROFILE} ;;
    r) REGION=${OPTARG:-$DEFAULT_REGION} ;;
    d) REPO=${OPTARG:-$DEFAULT_REPO} ;;
    h)
        usage
        exit
        ;;
    \?)
        echo "Unknown Option ${OPTARG} at ${OPTIND} "
        exit 10
        ;;
    :)
        echo "Missing Argument for ${OPTARG} at ${OPTIND} "
        exit 20
        ;;
    *)
        echo "Option Not Implemented"
        exit 30
        ;;
    esac
done
OPTIND=1

# declare -a REPOSITORIES=$(find . -maxdepth 1 -name "orion*" -type d | sed "s/.\///")
declare -a REPOSITORIES=$REPO

CICD_ACCOUNT=$(aws sts get-caller-identity --query "Account" --output text --profile ${CICD_PROFILE})
CHILD_ACCOUNT=$(aws sts get-caller-identity --query "Account" --output text --profile ${CHILD_PROFILE})

delete_repositories
