import os
import stat
import uuid

from bin.global_settings import TMP_DIR_PATH


class CustomerKeySaver:

    CUSTOMER_KEY_FILE = TMP_DIR_PATH + "/saved_customer_keys_for_subscriber_type_"
    FILE_EXTENSION = ".txt"
    
    def __init__(self, subscriber_type):
        self.subscriber_type = subscriber_type
        self.customer_key_list = []
        self.filename = CustomerKeySaver.CUSTOMER_KEY_FILE + subscriber_type.__str__() + CustomerKeySaver.FILE_EXTENSION
        
    def read(self):
        read_fd = None
        try :
            read_fd = open(self.filename, 'rb')
            if(read_fd == None):
                pass  
            # Read from file 
            for line in read_fd:
                line = line.strip()
                if len(line) < 1:
                    continue
                tp = (uuid.UUID(line),)
                self.customer_key_list.append(tp)
                
        except Exception as err:
            print err
        finally:
            self.__close_file(read_fd)
    
    def store(self, customer_key):
        self.customer_key_list.append(customer_key)
    
    def save(self):
        write_fd = None
        try:
            # If file exist, then modify the permissions
            if os.path.isfile(self.filename):
                os.chmod(self.filename, stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)

            write_fd = open(self.filename, 'w')
            if(write_fd != None):
                write_fd.write('\n'.join(str(customer_key) for customer_key in self.customer_key_list))
        except Exception as err:
            print(err)
        finally:
            self.__close_file(write_fd)
            os.chmod(self.filename, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
            self.customer_key_list[:] = []
            
    def get_customer_key_list(self):
        return self.customer_key_list
    
    def __close_file(self, fd):
        if None != fd:
            try:
                fd.close()
            except Exception, e:
                print("Error closing open file descriptor. Error: %s" % (e))