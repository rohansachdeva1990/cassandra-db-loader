import datetime

class CommonUtils:
            
    @staticmethod
    def get_padded_number(value, length):
        value_str = str(value)
        value_str_length = value_str.__len__()
        if value_str_length == length:
            return value_str
        else:
            return "0" * (length - value_str_length) + value_str

    @staticmethod
    def unix_time(dt):
        epoch = datetime.datetime.utcfromtimestamp(0)
        delta = dt - epoch
        return delta.total_seconds()

    @staticmethod
    def unix_time_millis(dt):
        return long(CommonUtils.unix_time(dt) * 1000.0)
    
    @staticmethod
    def represent_int(s):
        try: 
            int(s)
            return True
        except ValueError:
            return False
    
    @staticmethod
    def get_first_from_iterable(iterable, default=None):
        if iterable:
            for item in iterable:
                return item
        return default
    
    

