#Name: Deepak Thipeswamy
#Course Number: CSE 6331
#Lab: Programming Assignment- 1

#Reference:
# 1. Source code provided in Class 1 and code sheet that was given in class

import glob
import sys
import pdb
import gnupg
import swiftclient
import StringIO
import os
from os.path import isfile, join

container_name = 'new-container'
def start():
	# Get user input
	option = raw_input('Enter 1 for upload, enter 2 for download: \n')
	if(int(option)==1):	
	    path = os.getcwd()+'/uploads/*.*'
	    files = glob.glob(path)
	    for curFile in files: 
	            with open(curFile) as f: 
	                file_upld(f)
	else:
		option = raw_input('Enter the file name with extension: \n')
		status = download_file(str(option))
		print status

def file_upld(f):
	print 'In file_upld'
	oriFilename = f.name.split("/")[-1]
	conn = get_conn()
	create_cont(conn)
    #delete_obj(f.filename, conn)
	encrypt(f)
	put_file(conn, oriFilename)
	list_obj(conn)     

def put_file(conn, oriFilename):
    print 'In Create File'    
    # Create the file on the container
    with open(oriFilename, 'r') as ex_file:
            d = ex_file.read()
    conn.put_object(container_name,
            oriFilename,
            contents= d,
            content_type='text/plain')
    print 'File created successfully'

def download_file(filename):
    print 'In download_file'
    # Download the file from the container 
    conn = get_conn()
    try:
    	obj = conn.get_object(container_name, filename)
    except:
    	return 'File not Found'
    fname = filename.split("_")[0]
    save_path = './downloads'
    completeName = os.path.join(save_path, fname)
    # Write the object to a temporary file
    with open(completeName+'_enc', 'w') as my_file:
            my_file.write(obj[1])
            my_file.close()
    gpg = gnupg.GPG(gnupghome='/usr/lib/gnupg/.gnupg')
    # Decrypt the file
    with open(completeName+'_enc', 'r') as my_file1:  
            d = my_file1.read()
            data = gpg.decrypt(d, passphrase='xxxxx')
            my_file1.close()
            print "\nObject %s downloaded successfully." % fname
    # Write the decrypted content to /downloads folder
    with open(completeName+'_decrypted.txt', 'w') as final_file:
    		final_file.write(str(data))
    		final_file.close()
		return 'Downloaded successfully'

def encrypt(curfile):
	# Encrypt the file
    print 'In Encrypt file'
    filename = curfile.name.split("/")[-1]
    # Get gnupg object
    gpg = gnupg.GPG(gnupghome='/usr/lib/gnupg/.gnupg')
    input_data = gpg.gen_key_input(key_type="RSA", key_length=128, passphrase='xxxxx')
    # Generate key
    key = gpg.gen_key(input_data)
    # Encrypt
    status = gpg.encrypt_file(curfile, str(key), always_trust=True, output=filename)
    print status.ok
                
def get_conn():
    # swift client connection object
    auth_url = 'https://identity.open.softlayer.com/v3'
    project_id = '25085d2bdb6c4b0ea73424af654f5623'
    user_id = 'f92543b6fb66460e8c1a72c1a9bca0f6'
    region_name = 'dallas'
    password = 'B*!F*?3LT37f{,Im'
    # Get connection object
    print 'In get_conn'
    conn = swiftclient.Connection(
            key=password,
            authurl=auth_url,
            auth_version='3',
            os_options={"project_id": project_id,
                        "user_id": user_id,
                        "region_name": region_name})
    print 'Got Connection'
    return conn

def create_cont(conn):
    print 'In create_cont'
    # Create a container
    conn.put_container(container_name)
    print "\nContainer %s created successfully." % container_name
        
def list_cont(conn):
    print 'In list_cont'    
    # List containers
    print ("\nContainer List:")
    for c in conn.get_account()[1]:
        print c['name']

def list_obj(conn):     
    print ("\nObject List:")
    # List all objects in the container
    for container in conn.get_account()[1]:
        for data in conn.get_container(container['name'])[1]:
            print 'object: {0}\t size: {1}\t date: {2}'.format(data['name'], data['bytes'], data['last_modified'])

def delete_obj(filename, conn):
    # Delete an object from the container
    conn.delete_object(container_name, filename)
    print "\nObject %s deleted successfully." % filename

def delete_cont(conn):  
	# Delete the container
    conn.delete_container(container_name)
    print "\nContainer %s deleted successfully.\n" % container_name

if __name__ == '__main__':
    start()