SHELL=/bin/bash
CICD=default
CHILD=default
REGION=$(shell aws configure get region --profile ${CICD})
ENV=dev
CHILD_STACKS=$(shell cdk list --notices=false | grep "sdlf-ddk-${ENV}/" | tr '\n' ' ')


.PHONY: empty_buckets delete_sdlf delete_sdlf_no_cicd delete_bootstrap delete_bootstrap_no_cicd delete_repositories delete_all_items


delete_all: empty_buckets delete_sdlf delete_bootstrap delete_repositories delete_all_items
delete_all_no_cicd: empty_buckets delete_sdlf_no_cicd delete_bootstrap_no_cicd delete_all_items


help:
	@echo "Helper for the aws-sdlf-ddk MakeFile";
	@echo "For clean up of the solution execute the following command";
	@echo "make delete_all CICD=\"<your_cicd_profile>\" CHILD=\"<your_child_profile>\"" REGION=\"<the_deployment_region>\" ENV=\"<environment>\";
	@echo "For clean up of the solution without cicd execute the following command";
	@echo "make delete_all_no_cicd CHILD=\"<your_child_profile>\"" REGION=\"<the_deployment_region>\" ENV=\"<environment>\";
	@echo "-------------------------------------------\n";



delete_repositories:
	./scripts/cleanup_scripts/delete_repositories.sh -s ${CICD} -t ${CHILD} -r ${REGION} -d sdlf-ddk-example

empty_buckets:	
	pushd scripts/cleanup_scripts; python3 ./list_items_to_delete.py ${ENV} ${CHILD}; popd;
	pushd scripts/cleanup_scripts; python3 ./empty_buckets.py ${CHILD}; popd;
	
delete_sdlf:
	cdk destroy ${CHILD_STACKS} --force --profile ${CHILD}

	cdk destroy sdlf-ddk-pipeline --force --profile ${CICD}

delete_sdlf_no_cicd:
	cdk destroy ${CHILD_STACKS} --force --profile ${CHILD}
	
delete_bootstrap:
	aws cloudformation delete-stack --stack-name DdkDevBootstrap --profile ${CHILD}
	aws cloudformation delete-stack --stack-name DdkCicdBootstrap --profile ${CICD}

delete_bootstrap_no_cicd:
	aws cloudformation delete-stack --stack-name DdkDevBootstrap --profile ${CHILD}

delete_all_items:
	sleep 120

	pushd scripts/cleanup_scripts; python3 ./list_items_to_delete.py ${ENV} ${CHILD}; popd;
	pushd scripts/cleanup_scripts; python3 ./delete_script.py ${CHILD}; popd;



