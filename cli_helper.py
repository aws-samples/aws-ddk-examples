import os
import subprocess
import getopt
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
            print(f"{pattern} is not available in python, please open an issue on aws-ddk-examples on github if you are interested or run python3 script.py -t 'list' to know about available patterns")
        elif(lang == "typescript" and pattern not in self.typescript_list ):
            print(f"{pattern} is not available in typescript, please open an issue on aws-ddk-examples on github if you are interested or run python3 script.py -t 'list' to know about available patterns")
    

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
        if not self.directory or not self.sparse_paths:
            print("Usage: python script.py -p <pattern_name> -l <language>")
            sys.exit(2)

        dir = self._create_directory(self.directory)
        print(dir)
        dir_parent = os.path.dirname(dir)
        print(dir_parent)
        pattern_name = self.sparse_paths.split('/')[0]
        print(pattern_name)

        self._initialize_git()
        self._clone_repo_with_sparse_checkout()
        self._clean_up(source_directory = f"{dir}/{pattern_name}", target_directory = f"{dir_parent}/{pattern_name}")

def main(argv):
    short_options = "ht:p:l:"
    long_options = ["help", "type=", "pattern=", "language="]

    pattern = None

    try:
        opts, args = getopt.getopt(argv, short_options, long_options)
    except getopt.GetoptError:
        print("invalid command")
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print("Usage: python script.py -p <pattern_name> -l <language>")
        elif opt in ("-t", "--type"):
            type = arg
            if(type not in ["init", "list"]):
                print("invalid type: Only init and list allowed")
                sys.exit(2)
        elif opt in ("-p", "--pattern"):
            pattern = arg
            pattern = pattern.lower()
        elif opt in ("-l", "--language"):
            lang = arg
            lang = lang.lower()
            if(lang not in ["python", "typescript"]):
                print("invalid lang: Only python and typescript allowed")
                sys.exit(2)

    if(type == "init"):
        sparse_paths = f"{pattern}/{lang}"
        list_pattern = ListPatterns()
        list_pattern.check_pattern(pattern=pattern, lang=lang)
        git_sparse_checkout = GitSpareseCheckout(repo_url, directory, sparse_paths)
        git_sparse_checkout.check_directory(pattern)
        git_sparse_checkout.clone()
    elif(type == "list"):
        if(pattern is not None):
            print("Invalid: For list -p and -l is not required")
        else:
            list_pattern = ListPatterns()
            list_pattern.print_patterns()


repo_url = 'https://github.com/aws-samples/aws-ddk-examples'
directory = 'aws-ddk-temp'

if __name__ == "__main__":
    main(sys.argv[1:])





        

        

        