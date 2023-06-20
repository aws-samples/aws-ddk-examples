import os
import subprocess
import argparse
import sys
import shutil
import urllib3
import json


class ListPatterns:
    def __init__(self):
        self.python_list=[]
        self.typescript_list=[]
        self._list_patterns()

    def _list_patterns(self):
        """list patterns and segregate them based on language"""
        http = urllib3.PoolManager()
        response = http.request('GET', 'https://raw.githubusercontent.com/aws-samples/aws-ddk-examples/feat/cli-helper/info.json')
        
        data = (json.loads(response.data)['patterns'])

        for pattern in data:
            if(data[pattern]["python"] == True):
                self.python_list.append(pattern)
            if(data[pattern]["typescript"] == True):
                self.typescript_list.append(pattern)

    def check_pattern(self, pattern, lang):
        """Initialize a git repo"""
        if(lang == "python" and pattern not in self.python_list):
            print(f"{pattern} is not available in python or is not a valid pattern")
            print("please open an issue on aws-ddk-examples on github if you are interested or run python3 script.py -t 'list' to know about available patterns")
            sys.exit(2)
        elif(lang == "typescript" and pattern not in self.typescript_list ):
            print(f"{pattern} is not available in typescript or is not a valid pattern")
            print("please open an issue on aws-ddk-examples on github if you are interested or run python3 script.py -t 'list' to know about available patterns")
            sys.exit(2)

    def print_patterns(self):
        """Print patterns based on languages"""
        if(len(self.python_list)>0):
            print("Python Patterns")
            print("---------------------------------------")
            for pattern in self.python_list:
                print(pattern)

        if(len(self.typescript_list)>0):
            print("\n")
            print("TypeScript Pattern")
            print("---------------------------------------")
            for pattern in self.typescript_list:
                print(pattern)



class GitSpareseCheckout:
    def __init__(self, repo_url, directory, sparse_paths):
        self.repo_url = repo_url
        self.directory = directory
        self.sparse_paths = sparse_paths

    def _create_directory(self, directory):
        """Creates a new directory"""
        os.makedirs(directory)
        return os.path.abspath(directory)
    
    def check_directory(self, directory):
        """Checks if a directory exists"""
        if os.path.exists(os.path.abspath(directory)):
            print(f'{directory} already exists')
            sys.exit(2)
    
    def _initialize_git(self):
        """Initialize a git repo"""
        subprocess.run(['git', 'init'], cwd=self.directory)

    def _clean_up(self, source_directory, target_directory):
        """Cleans up temp directory used for this proccess"""
        try:
            shutil.move(source_directory, target_directory)
        except:
            os.makedirs(target_directory)
            shutil.move(source_directory, target_directory)

        subprocess.run(['rm', '-r', '-f', 'aws-ddk-temp'])

    def _clone_repo_with_sparse_checkout(self):
        """Clones a repo with sparse checkout for the pattern specified"""
        subprocess.run(['git', 'remote', 'add', 'origin', '-f', self.repo_url], cwd=self.directory)

        subprocess.run(['git', 'config', 'core.sparsecheckout', 'true'], cwd=self.directory)

        sparse_checkout_path = os.path.join(self.directory, '.git', 'info', 'sparse-checkout')
        with open(sparse_checkout_path, 'w') as sparse_file:
            sparse_file.write(self.sparse_paths)

        
        subprocess.run(['git', 'pull' , 'origin', 'main'], cwd=self.directory)

    def clone(self):
        """Start the process for cloning"""
       
        dir = self._create_directory(self.directory)
        dir_parent = os.path.dirname(dir)
        pattern_name = self.sparse_paths.split('/')[0]

        self._initialize_git()
        self._clone_repo_with_sparse_checkout()
        self._clean_up(source_directory = f"{dir}/{pattern_name}", target_directory = f"{dir_parent}/{pattern_name}")

def main():
    pattern = None
    
    parser = argparse.ArgumentParser(description="Python CLI for DDK example pipeline")

    parser.add_argument('-t', '--type', choices=['init', 'list'], help='Specify type (init or list)', required=True)
    parser.add_argument('-p', '--pattern', help='Choose from available patterns', required=False)
    parser.add_argument('-l', '--language', choices=['python', 'typescript'], help='Specify language (python or typescript)', required=False)

    args = parser.parse_args()
    type = args.type
    pattern = args.pattern
    lang = args.language
  

    if(type == "init"):
        sparse_paths = f"{pattern}/{lang}"
        list_pattern = ListPatterns()
        list_pattern.check_pattern(pattern=pattern, lang=lang)
        git_sparse_checkout = GitSpareseCheckout(repo_url='https://github.com/aws-samples/aws-ddk-examples', directory='aws-ddk-temp', sparse_paths=sparse_paths)
        git_sparse_checkout.check_directory(pattern)
        git_sparse_checkout.clone()
    elif(type == "list"):
        if(pattern is not None or lang is not None):
            print("Invalid: For list -p/--pattern and -l/--language is not required")
        else:
            list_pattern = ListPatterns()
            list_pattern.print_patterns()

if __name__ == "__main__":
    main()





        

        

        