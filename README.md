# Tiny Data

## Instructions to Run
### On Master
1. In the top-level Tiny Data directory run `python master`
2. Look up and write down the public-facing IP address of this machine

### On Followers
1. In the top-level Tiny Data directory run `python follower master_ip` where `master_ip` is the IP address recorded before.

### On Client
1. In the top-level Tiny Data directory run `python client master_ip` where `master_ip` is the IP address recorded before. This will print out a list of commands that are available.
2. Here's an example of every command. All paths in the DFS **must** be provided as absolute paths, starting at root (/)
    * ls - `python client master_ip ls /` 
    * rm - `python client master_ip rm /some_data_file`
    * mkdir - `python client master_ip mkdir /my_dir`
    * cat - `python client master_ip cat /some_results_file`
    * upload - `python client master_ip upload /data_path_on_dfs ~/data_on_my_computer 1000`
    * map_reduce - `python client master_ip map_reduce /data_path /store_results_here map_file.py reduce_file.py --combine combine_file.py`

## Example Word Count MapReduce Job File
```python
import re

def map_fn(line):
    words = re.findall(r"[\w']+", line)
    return [(word.strip().lower(), 1) for word in words]

def reduce_fn(key, values):
    return sum(values)
```