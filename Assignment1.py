import glob
import pdb
import gnupg
import swiftclient
import StringIO
import os
from os.path import isfile, join

container_name = 'new-container'
def start():
    path = '/home/hadoop/cloud1/uploads/*.*'
    files = glob.glob(path)
    for curFile in files: # 'file' is a builtin type, 'name' is a less-ambiguous variable name.
            with open(curFile) as f: # No need to specify 'r': this is the default.
                #sys.stdout.write(f.read())
                file_upld(f)

def file_upld(f):
    #pdb.set_trace()
    print 'In file_upld'
    conn = get_conn()       
    create_cont(conn)
    encrypt(f)
    filename = put_file(conn, f)
    list_obj(conn)     
    download_file(filename)

def put_file(conn, f):
# creates the file if not present and opens in write mode / reads the existing file  
    print 'In Create File'
    filename = f.name.split("/")[-1]+'_enc'
    #pdb.set_trace()
    with open(filename, 'r') as ex_file:
            d = ex_file.read()
    conn.put_object(container_name,
            filename,
            contents= d,
            content_type='text/plain')
    print 'File created successfully'
    return filename 

def download_file(filename):
    # Download an object and save it to ./my_file.txt
    print 'In download_file'
    conn = get_conn()
    obj = conn.get_object(container_name, filename)
    #pdb.set_trace()
    fname = filename.split("_")[0]
    save_path = './downloads'
    completeName = os.path.join(save_path, fname)
    with open(completeName+'_enc', 'w') as my_file:
            my_file.write(obj[1])
            my_file.close()
    gpg = gnupg.GPG(gnupghome='/usr/lib/gnupg/.gnupg')
    with open(completeName+'_enc', 'r') as my_file1:  
            d = my_file1.read()
            data = gpg.decrypt(d, passphrase='xxxxx')
            my_file1.close()
            print "\nObject %s downloaded successfully." % fname
    with open(completeName+'_decrypted.txt', 'w') as final_file:
    		final_file.write(str(data))

def encrypt(curfile):
    filename = curfile.name.split("/")[-1]
    print 'In Encrypt file'
    #pdb.set_trace()
    gpg = gnupg.GPG(gnupghome='/usr/lib/gnupg/.gnupg')
    input_data = gpg.gen_key_input(key_type="RSA", key_length=128, passphrase='xxxxx')
    key = gpg.gen_key(input_data)
    status = gpg.encrypt_file(curfile, str(key), always_trust=True, output=filename+'_enc')
    print status.ok
                
def get_conn():
    # swift client connection object
    auth_url = 'https://identity.open.softlayer.com/v3'
    # OpenStack project unique id for IBM Bluemix authentication service
    project_id = '25085d2bdb6c4b0ea73424af654f5623'
    user_id = 'f92543b6fb66460e8c1a72c1a9bca0f6'
    region_name = 'dallas'
    password = 'B*!F*?3LT37f{,Im'
    #pdb.set_trace()
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
    conn.put_container(container_name)
    print "\nContainer %s created successfully." % container_name
        
def list_cont(conn):
    print 'In list_cont'    
    # List containers
    print ("\nContainer List:")
    for c in conn.get_account()[1]:
        print c['name']

def list_obj(conn):     
    # List objects in a container, and prints out each object name, the file size, and last modified date
    print ("\nObject List:")
    for container in conn.get_account()[1]:
        for data in conn.get_container(container['name'])[1]:
            print 'object: {0}\t size: {1}\t date: {2}'.format(data['name'], data['bytes'], data['last_modified'])

def delete_obj(filename, conn):
    # Delete an object
    conn.delete_object(container_name, filename)
    print "\nObject %s deleted successfully." % filename

def delete_cont(conn):  
    # To delete a container. Note: The container must be empty!
    conn.delete_container(container_name)
    print "\nContainer %s deleted successfully.\n" % container_name


if __name__ == '__main__':
    start()