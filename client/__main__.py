import argparse

from client.threads import ClientThread


parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(dest='command')

ls_parser = subparsers.add_parser('ls')
ls_parser.add_argument('path', help='List the contents of the directory in the DFS at this path')

rm_parser = subparsers.add_parser('rm')
rm_parser.add_argument('path', help='Remove the file in the DFS at this path')

mkdir_parser = subparsers.add_parser('mkdir')
mkdir_parser.add_argument('path', help='Create a directory in the DFS at this path')

ls_parser = subparsers.add_parser('cat')
ls_parser.add_argument('path', help='Get the contents of the file in the DFS at this path')

map_reduce_parser = subparsers.add_parser('map_reduce')
map_reduce_parser.add_argument('path', help='Run the map reduce job over the file in the DFS at this path')
map_reduce_parser.add_argument('results_path', help='Store the results in the DFS at this path')
map_reduce_parser.add_argument('job_path', help='A python map reduce job file, which defines map_fn and reduce_fn')

upload_parser = subparsers.add_parser('upload')
upload_parser.add_argument('path', help='Store the data in the DFS at this path')
upload_parser.add_argument('local_path', help='Upload the file at this path')
upload_parser.add_argument('lines_per_chunk', help='The number of lines to include in each chunk')

def main():
    args = parser.parse_args()

    c_thread = ClientThread()
    if args.command in ['ls', 'rm', 'mkdir', 'cat']:
        c_thread.send_simple(args.command, args.path)
    elif args.command is 'map_reduce':
        c_thread.send_map_reduce(args.path, args.results_path, args.job_path)
    elif args.command is 'upload':
        c_thread.send_upload(args.path, args.local_path, args.lines_per_chunk)
    c_thread.start()
    c_thread.join()

if __name__ == '__main__':
    main()