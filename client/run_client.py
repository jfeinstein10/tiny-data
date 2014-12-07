import os



while True:
    arg_list = raw_input().strip().split()
    command_line = 'python client'
    for arg in arg_list:
        command_line += ' ' + arg
    os.system(command_line)