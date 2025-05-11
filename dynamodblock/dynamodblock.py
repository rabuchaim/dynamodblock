#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
 ###  #   #  #  #   ##   #  #   ##   ###   ###         #      ##    ###  #  #
 #  #  # #   ## #  #  #  ####  #  #  #  #  #  #        #     #  #  #     # #
 #  #   #    # ##  #  #  ####  #  #  #  #  ###         #     #  #  #     ##
 #  #   #    #  #  ####  #  #  #  #  #  #  #  #        #     #  #  #     # #
 ###    #    #  #  #  #  #  #   ##   ###   ###         ####   ##    ###  #  #

       ####   ##   ###         #      ##   #  #  ###   ###    ##    ###
       #     #  #  #  #        #     #  #  ####  #  #  #  #  #  #  #
       ###   #  #  ###         #     #  #  ####  ###   #  #  #  #   ##
       #     #  #  # #         #     ####  #  #  #  #  #  #  ####     #
       #      ##   #  #        ####  #  #  #  #  ###   ###   #  #  ###

DynamoDBLock is a distributed locking mechanism using DynamoDB. It's designed 
for scenarios where multiple concurrent Lambda executions need to ensure that 
certain tasks are performed exclusively by a single lambda execution.

Author.: Ricardo Abuchaim - ricardoabuchaim@gmail.com
Github.: http://github.com/rabuchaim/dynamodblock
Issues.: https://github.com/rabuchaim/dynamodblock/issues
PyPI...: https://pypi.org/project/dynamodblock/  ( pip install dynamodblock )
Version: 1.0.2 - Release Date: 11/May/2025
License: MIT

"""
import os, boto3, time, contextlib, datetime, multiprocessing, math, json, functools
from uuid import uuid4
from collections import namedtuple
from abc import ABC, abstractmethod
from threading import Lock
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Attr, Or, Key
from boto3.dynamodb.table import TableResource as DynamoDBTableResource 
from botocore.client import BaseClient

__appname__ = "DynamoDB Lock for Lambdas" 
__version__ = "1.0.2"
__release__ = "11/May/2025"

__all__ = ['DynamoDBLock','create_dynamodb_table',
           'DynamoDBLockException','DynamoDBLockTimeoutError','DynamoDBLockWarmUpException','DynamoDBLockAcquireException',
           'DynamoDBLockReleaseException','DynamoDBLockGetLockException','DynamoDBLockPutLockException',
           'ElapsedTimer','SafeTimeoutDecorator','SafeTimeoutError']

class DynamoDBLockException(Exception):...        # Raised when the lock mechanism fails.
class DynamoDBLockTimeoutError(Exception):...     # Raised when the lock timeout is reached.
class DynamoDBLockWarmUpException(Exception):...  # Raised when the warmup method fails.
class DynamoDBLockAcquireException(Exception):... # Raised when the lock acquire method fails.
class DynamoDBLockReleaseException(Exception):... # Raised when the lock release method fails.
class DynamoDBLockGetLockException(Exception):... # Raised when the lock get method fails.
class DynamoDBLockPutLockException(Exception):... # Raised when the lock put method fails.

class ElapsedTimer:
    """A simple context manager to measure the elapsed time in seconds.
       
       Usage: 
            with ElapsedTimer() as elapsed:
                print(elapsed.text(decimal_places=6, end_text=" seconds.", with_brackets=False))
    """
    def __enter__(self):
        self.start = time.monotonic()
        self.time = None
        return self
    def __exit__(self, type, value, traceback):
        self.time = time.monotonic() - self.start
    def time_as_float(self,decimal_places:int=6)->float:
        return math.trunc((time.monotonic()-self.start)*(10**decimal_places))/(10**decimal_places)
    def text(self,decimal_places:int=6,end_text:str=" sec",begin_text:str="",with_brackets=True):
        if self.time is None: self.time = time.monotonic() - self.start
        timer_string = f"[{f'%.{decimal_places}f'%(self.time)}{end_text}]"
        try: return timer_string if with_brackets else timer_string[1:-1]
        finally: self.time = None

class SafeTimeoutError(Exception):...
def SafeTimeoutDecorator(timeout:float,*,error_message="Safe timeout exceeded"):
    """ A safe timeout based on multiprocessing. """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args,**kwargs):
            def target(queue,*target_args,**target_kwargs):
                try:
                    result = func(*target_args,**target_kwargs)
                    queue.put((True,result))
                except Exception as ERR:
                    queue.put((False,ERR))
            queue = multiprocessing.Queue()
            p = multiprocessing.Process(target=target,args=(queue,*args),kwargs=kwargs)
            p.start()
            p.join(timeout)
            if p.is_alive():
                p.terminate()
                p.join()
                raise SafeTimeoutError(error_message) from None
            success, value = queue.get() if not queue.empty() else (False,SafeTimeoutError("Function exited unexpectedly"))
            if success:
                return value
            else:
                raise value
        return wrapper
    return decorator

class DynamoDBLockAcquireReturnProxy():
    """A context aware object that will release the lock file when exiting. From python lockfile"""
    def __init__(self, lock: DynamoDBLockAcquireReturnProxy)->None:
        self.lock = lock
    def __enter__(self)->DynamoDBLockAcquireReturnProxy:
        return self.lock
    def __exit__(self, exc_type, exc_value, traceback)->None:
        self.lock.release()

class DynamoDBLockLogging():
    """Class for logging. Create a new class with the same methods and attributes to customize the logging mechanism.
       
        class myLoggingClass(DynamoDBLockLogging):
            def info(self,msg,prefix:str="[INFO] ")->None:  
                pass # customize as you wish
            def debug(self,msg,prefix:str="[DEBUG] ")->None:
                pass # customize as you wish

    You need to create a new class with method `_get_logger(self)->myLoggingClass:` returning your new DynamoDBLockLogging class.
    
        class DynamoDBLockMyDatabase(DynamoDBLockBase):
            def _get_logger(self)->DynamoDBLockLogging:
                return myLoggingClass(verbose=self.__verbose,debug=self.__debug)
                
    """
    def __init__(self,verbose:bool=False,debug:bool=False,info_prefix="[INFO] ",debug_prefix="[DEBUG] ",with_date:bool=True)->None:
        self.__debug = debug
        self.__verbose = verbose
        self.info_prefix = info_prefix
        self.debug_prefix = debug_prefix
        self.__with_date = with_date
        self.info = self.__logEmpty if not self.__verbose else self.info
        self.debug = self.__logEmpty if not self.__debug else self.debug
    def info(self,msg,prefix:str=None)->None:
        print(f"{self.__get_date()}{prefix if prefix is not None else self.info_prefix}{msg}",flush=True)
    def debug(self,msg,prefix:str=None)->None:
        print(f"{self.__get_date()}{prefix if prefix is not None else self.debug_prefix}{msg}",flush=True)
    def __logEmpty(self,msg,prefix:str="")->None:...
    def __get_date(self):
        if not self.__with_date: return ''
        A = datetime.now()
        if A.microsecond%1000>=500:A=A+timedelta(milliseconds=1)
        D = A.strftime('%y/%m/%d %H:%M:%S.%f')[:-3]
        return D+" "

class DynamoDBLockBaseForDynamoDB(ABC, contextlib.ContextDecorator):
    """ Base class for implementing distributed locking using AWS DynamoDB. """
    def __init__(self, 
                 lock_id:str,                                   # a unique identifier for the lock 
                 dynamodb_table_resource:DynamoDBTableResource, # a DynamoDB Table Resource previously configured with your access credentials, region and extra settings
                 lock_ttl:int=60,                               # time-to-live for the lock in seconds
                 retry_timeout:float=10.0,                      # timeout for trying to acquire the lock in seconds. Min: 0.5
                 retry_interval:float=1.0,                      # minimum time to wait is 0.1 seconds and max is timeout value
                 owner_id:str=None,                             # an identifier for the owner of the lock. Could be "context.aws_request_id"
                 warmup:bool=False,                             # warmup lock table on init
                 timezone:str=None,                             # specific timezone for ttl/datetime manipulations. Default is the environment variable TZ
                 verbose:bool=False,                            # print messages
                 debug:bool=False,                              # print debug messages
                 )->None:

        if not isinstance(dynamodb_table_resource,DynamoDBTableResource):
            raise AttributeError(f"The provided dynamodb_table_resource parameter is not a valid boto3.dynamodb.table.TableResource (current class {type(dynamodb_table_resource)})") from None
        try:
            self.__lock_region = dynamodb_table_resource.meta.client.meta.region_name
        except Exception as ERR:
            raise DynamoDBLockException(f"Could not get the region of provided dynamodb_table_resource. {str(ERR)}")

        self.LockInfo = namedtuple("LockInfo", ["lock_id", "lock_region", "ttl", "expire_datetime", "expire_datestring", "owner_id", "return_code", "return_message", "elapsed_time"], defaults=[None,None,None,None,None,None,None,None,None])

        self.__debug = debug
        self.__verbose = verbose
        self.__classLogging = self.__get_logger(self.__verbose,self.__debug)
        self.logDebug = self.__classLogging.debug
        self.logInfo = self.__classLogging.info

        self._threadsafe_put_lock:Lock = Lock()
        self._threadsafe_delete_lock:Lock = Lock()
        self.__should_delete_lock = True
        
        self.lock_id = lock_id
        self.lock_ttl = lock_ttl
        self.__owner_id = owner_id if owner_id is not None else str(uuid4())[:8]
        self.__lock_ttl = None
        
        self.retry_timeout = retry_timeout if retry_timeout >= 0.5 else 0.5
        self.retry_interval = retry_interval if retry_interval >= 0.1 else 0.1

        if self.retry_interval >= self.retry_timeout:
            raise ValueError("retry_interval must be less or equal than retry_timeout") from None

        with ElapsedTimer() as elapsed_initial:
            self.logInfo(f"Initializing {self.__class__.__name__}...    ")
            ##──── call set_timezone() only if the given timezone is different from the environment variable TZ ──────────────────────────────
            self.__current_timezone = os.getenv("TZ",None)
            if (timezone is not None) and (timezone != self.__current_timezone): 
                self.set_timezone(timezone)
            else:
                self.__current_timezone, _ = time.tzname
            
            self.ddb_table = dynamodb_table_resource
            ##──── Call the warmup() method to check if the table is available and working. ──────────────────────────────────────────────────
            if warmup:
                try:
                    if not self.warmup():
                        raise
                except Exception as ERR:    
                    raise DynamoDBLockWarmUpException(f"{str(ERR)}. Check your access to AWS and the resources and try again.") from None
            else:
                self.__test_ddb_access()
        
            ##──── Print the initial configuration of the lock mechanism if VERBOSE or DEBUG is enabled. ─────────────────────────────────────
            self.logInfo(f"Initialized {self.__class__.__name__}: lock_id='{self.lock_id}' ddb_table='{self.ddb_table}' owner_id='{self.get_owner_id()}' "
                        f"ttl={self.lock_ttl} retry_timeout={self.retry_timeout} retry_interval={self.retry_interval} {elapsed_initial.text()}")
        
    def __get_logger(self,verbose,debug)->DynamoDBLockLogging:
        """You can customize the logging mechanism by creating a new class with the same methods and attributes."""
        return DynamoDBLockLogging(verbose=verbose,debug=debug)

    def __test_ddb_access(self)->None:
        with ElapsedTimer() as elapsed:
            try:
                self.ddb_table.load()
                self.logDebug(f"Access to table '{self.ddb_table.name}' is OK! {elapsed.text()}.")
                return True
            except Exception as ERR:
                self.logDebug(f"Failed to access to table '{self.ddb_table.name}' {str(ERR)} {elapsed.text()}.")
                raise DynamoDBLockException(str(ERR)) from None

    def set_timezone(self,timezone:str)->bool:
        """Set the timezone for the lock mechanism. The timezone is used to calculate the expiration time of the lock."""
        with ElapsedTimer() as elapsed:
            try:
                self.__original_timezone = os.environ.get("TZ",None)
                self.__current_timezone = timezone
                os.environ["TZ"] = self.__current_timezone
                time.tzset()
                self.logDebug(f"Time zone adjusted to '{timezone}' (was '{self.__original_timezone}'). Now is {time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime())} {elapsed.text()}.")
                return True
            except Exception as ERR:
                if self.__original_timezone is not None:
                    self.__current_timezone = self.__original_timezone
                    os.environ["TZ"] = self.__current_timezone
                    time.tzset()
                    self.logDebug(f"Failed to set time zone '{timezone}'. Rolling back to the default timezone ({self.__original_timezone}). Now is {time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime())}. Error message: {str(ERR)} {elapsed.text()}")
                return False
        
    def get_current_timezone(self)->str:
        """Return the current timezone."""
        return self.__current_timezone
    
    def warmup(self)->bool:
        """Warm-up the lock table to check if it is available and working. This method is called by the __init__() method and 
        should return True or can be disabled by setting the 'warmup' parameter to False.
        
        This method is important to check if the table is available and working before the lock mechanism starts to work.
        """
        warmup_lock_id = f'warmup_lock_{str(uuid4())}'
        warmup_lock_ttl = int(time.time())+2
        try:
            with ElapsedTimer() as elapsed1:
                self.ddb_table.put_item(Item={"lock_id": warmup_lock_id, "ttl": warmup_lock_ttl})
                self.logDebug(f"Warm-Up put_item '{warmup_lock_id}' (ttl {warmup_lock_ttl}) on table '{self.ddb_table.name}' in {elapsed1.text(with_brackets=False)}.")
        except Exception as ERR:
            raise DynamoDBLockWarmUpException(f"Failed at warm-up put_items! {str(ERR)}") from None
        try:
            with ElapsedTimer() as elapsed2:
                response = self.ddb_table.query(KeyConditionExpression=Key("lock_id").eq(warmup_lock_id),Limit=1).get("Items",[])
                response = {k: v for d in response for k, v in d.items()}
                if response != [] and response.get('ttl',0) == warmup_lock_ttl:
                    self.logDebug(f"Warm-Up query on table '{self.ddb_table.name}' in {elapsed2.text(with_brackets=False)}.")
                else:
                    raise Exception(f"Error in query response")
        except Exception as ERR:
            raise DynamoDBLockWarmUpException(f"Failed at warm-up query! {str(ERR)}") from None
        try:
            with ElapsedTimer() as elapsed3:
                self.ddb_table.delete_item(Key={"lock_id":warmup_lock_id})
                self.logDebug(f"Warm-Up delete_item on table '{self.ddb_table.name}' in {elapsed3.text(with_brackets=False)}.")
                self.logInfo(f"Warm-Up finished in {elapsed1.text(with_brackets=False)}.")
        except Exception as ERR:
            raise DynamoDBLockWarmUpException(f"Failed at warm-up delete_items! {str(ERR)}") from None
        return True
        
    def __enter__(self):
        """Context manager to acquire the lock."""
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager to release the lock."""
        self.release()
    
    def get_owner_id(self)->str:
        """Return the owner_id of the lock."""
        return self.__owner_id

    @property
    def is_locked(self)->bool:
        """Return True if the lock is acquired by another process."""
        return self.__check_lock()

    @abstractmethod
    def _acquire(self, force:bool=False)->None:
        raise NotImplementedError

    @abstractmethod
    def _release(self)->None:
        raise NotImplementedError

    def release(self,force:bool=False, raise_on_exception:bool=False)->bool:
        return self.__delete_lock(force=force, raise_on_exception=raise_on_exception) 

    def acquire(self, force:bool=False, lock_ttl:int|None=None, retry_timeout:int|None=None, retry_interval:float|None=None)->DynamoDBLockAcquireReturnProxy:
        """Acquire the lock. If the lock is already acquired by another process, it will wait until the lock is released or the retry_timeout is reached.
        
        Use the 'force' parameter to acquire the lock by force, ignoring if the lock is already acquired by another process.
        """
        start_time = time.monotonic()
        lock_ttl = lock_ttl if lock_ttl is not None else self.lock_ttl
        retry_timeout = retry_timeout if retry_timeout is not None else self.retry_timeout
        retry_interval = retry_interval if retry_interval is not None else self.retry_interval
        try:
            if force:
                if self.__put_lock(force, lock_ttl):
                    return DynamoDBLockAcquireReturnProxy(lock=self)
                raise DynamoDBLockAcquireException(f"Failed to force acquire()") from None
            else:
                while True:
                    with ElapsedTimer() as elapsed:
                        if not self.is_locked:
                            if self.__put_lock(force, lock_ttl):
                                return DynamoDBLockAcquireReturnProxy(lock=self)
                        self.logDebug(f"Lock '{self.lock_id}' is already acquired by another process. Waiting {retry_interval} seconds to try again... {elapsed.text()}")
                        time.sleep(retry_interval)
                        if 0 <= retry_timeout <= (time.monotonic() - start_time):
                            self.__should_delete_lock = False
                            raise DynamoDBLockTimeoutError(f"Timed out on acquiring lock '{self.lock_id}'.") from None
        except DynamoDBLockTimeoutError as ERR:
            raise DynamoDBLockTimeoutError(str(ERR)) from None
        except Exception as ERR:
            raise DynamoDBLockAcquireException(str(ERR)) from None

    def __put_lock(self,force:bool=False,lock_ttl:int|None=None)->bool:
        """Put the lock in the DynamoDB table. If the lock is already acquired by another process, it will return False."""
        try:
            with ElapsedTimer() as elapsed: 
                lock_ttl = lock_ttl if lock_ttl is not None else self.lock_ttl
                if force:
                    response = self.ddb_table.put_item(Item={"lock_id": self.lock_id, "lock_region": self.__lock_region, "ttl": int(time.time()) + lock_ttl, "owner_id": self.get_owner_id()})
                else:
                    with self._threadsafe_put_lock:
                        self.__lock_ttl = int(time.time()) + lock_ttl
                        response = self.ddb_table.put_item(Item={"lock_id": self.lock_id, 
                                                                 "lock_region": self.__lock_region, 
                                                                 "ttl": self.__lock_ttl, 
                                                                 "owner_id": self.get_owner_id()}, 
                                                           ConditionExpression=Or(Attr("lock_id").not_exists(),Attr("ttl").lt(int(time.time()))))
                if result := (response.get("ResponseMetadata",{}).get("HTTPStatusCode",0) == 200):
                    self.logDebug(f"Lock '{self.lock_id}' successfully acquired {'by force ' if force else ''}{elapsed.text()}")
                return result
        except Exception as ERR:
            raise DynamoDBLockPutLockException(str(ERR)) from None

    def get_lock_info(self)->namedtuple:
        """Return a namedtuple LockInfo with the lock information."""
        with ElapsedTimer() as elapsed: 
            try:
                item_lock = self.ddb_table.query(KeyConditionExpression=Key("lock_id").eq(self.lock_id),Limit=1).get("Items",[])
                if item_lock != []:
                    ttl_datetime = datetime.fromtimestamp(int(item_lock[0].get("ttl",0)))
                    return self.LockInfo(lock_id=self.lock_id,
                                         lock_region=item_lock[0].get("lock_region",""),
                                         ttl=int(item_lock[0].get("ttl",0)),
                                         expire_datetime=ttl_datetime,
                                         expire_datestring=ttl_datetime.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z"),
                                         owner_id=item_lock[0].get("owner_id",""),
                                         return_code=200,
                                         return_message="OK",
                                         elapsed_time=elapsed.time_as_float(6)
                                        )
                else:
                    return self.LockInfo(return_code=404,return_message="Lock Not Found",elapsed_time=elapsed.time_as_float())
            except Exception as ERR:
                return self.LockInfo(return_code=500,return_message=f"Internal error {str(ERR)}",elapsed_time=elapsed.time_as_float())

    def __check_lock(self)->bool:
        """Get the lock information from the DynamoDB table. If the lock is acquired by another process, it will return True."""
        try:
            item_lock = self.ddb_table.query(KeyConditionExpression=Key("lock_id").eq(self.lock_id),Limit=1).get("Items",[])
            if item_lock != []:
                ##──── Get the existence of the lock and check if it is expired 
                if int(item_lock[0].get("ttl",0)) > time.time():
                    return True
                else:
                    ##──── the lock should be expired, so we can delete it by force and ignore if it fails 
                    try: self.ddb_table.delete_item(Key={"lock_id": self.lock_id})
                    except: pass  
            return False
        except Exception as ERR:
            raise DynamoDBLockGetLockException(str(ERR)) from None

    def __delete_lock(self,force:bool=False, raise_on_exception:bool=False)->bool:
        with ElapsedTimer() as elapsed: 
            with self._threadsafe_delete_lock:
                try:
                    if self.__should_delete_lock or force:
                        if force:
                            response = self.ddb_table.delete_item(Key={"lock_id": self.lock_id})
                        else:
                            # o bloqueio deve existir e o owner_id deve ser o mesmo que o owner_id atual, e o ttl e a lock_region
                            info = self.get_lock_info()
                            if (info.lock_id,info.lock_region,info.ttl,info.owner_id) == (self.lock_id,self.__lock_region,self.__lock_ttl,self.__owner_id):
                                # yes, this lock belongs to me, so I will delete it
                                response = self.ddb_table.delete_item(Key={"lock_id": self.lock_id},ConditionExpression=Attr("lock_id").eq(self.lock_id))
                            else:
                                self.logDebug(f"The current lock does not belong to this session... lock release aborted ({info})")
                        if response.get("ResponseMetadata",{}).get("HTTPStatusCode",0) == 200:
                            self.__should_delete_lock = False
                            self.logDebug(f"Lock '{self.lock_id}' successfully released{' by force' if force else ''} {elapsed.text()}")
                            return True
                except Exception as ERR:
                    if not raise_on_exception:
                        return False
                    raise DynamoDBLockReleaseException(str(ERR)) from None
        return False

class DynamoDBLock(DynamoDBLockBaseForDynamoDB):
    """
    This class provides a flexible foundation for building locking mechanisms
    backed by a DynamoDB table. It is designed to be used with AWS Lambda or other
    distributed systems that require mutual exclusion or lease-based locks. It supports
    TTL-based lock expiration, retries for contention handling, and contextual usage via
    the `with` statement.

    Parameters:
        lock_id (str): 
            A unique identifier for the lock. This serves as the partition key in the DynamoDB table.
        
        dynamodb_table_resource (DynamoDBTableResource): 
            A DynamoDB Table Resource object previously configured with credentials, region, and settings.
        
        lock_ttl (int, default=60): 
            Time-to-live for the lock in seconds. Determines how long the lock will stay valid.
        
        retry_timeout (float, default=10.0): 
            Maximum time (in seconds) to keep retrying to acquire the lock if it's already held.
            Minimum allowed value is 0.5 seconds.
        
        retry_interval (float, default=1.0): 
            Time (in seconds) between retries while waiting for the lock to become available.
            Minimum is 0.1 seconds, and it should be less than or equal to `retry_timeout`.
        
        owner_id (str, optional): 
            Optional identifier for the lock owner, useful for debugging or when using contextual info
            such as `context.aws_request_id`.
        
        warmup (bool, default=False): 
            If True, performs a warmup operation on the DynamoDB table during initialization.
        
        timezone (str, optional): 
            Timezone to be used for TTL and datetime manipulations. If not provided, the value from
            the environment variable `TZ` will be used.
        
        verbose (bool, default=False): 
            If True, prints user-facing informational messages during operation.
        
        debug (bool, default=False): 
            If True, enables verbose debug output, including internal decisions and retries.

    Usage:
    
        import boto3
        from dynamodblock import DynamoDBLock
        client = boto3.resource("dynamodb")
        table = client.Table("locks-table")
    
        with DynamoDBLock(lock_id='my_lock',dynamodb_table_resource=table) as lock:
            # Critical section
            do_something_exclusive()

        my_lock = DynamoDBLock(lock_id='my_lock',dynamodb_table_resource=table)
        with my_lock.acquire():
            # Critical section
            do_something_exclusive()

        my_lock = DynamoDBLock(lock_id='my_lock',dynamodb_table_resource=table)
        try:
            my_lock.acquire()
            # Critical section
            do_something_exclusive()
        finally:
            my_lock.release()

    """
    def _acquire(self, force:bool=False)->None:...
    def _release(self)->None:...

def create_dynamodb_table(table_name:str,boto3_client:BaseClient,verbose:bool=True,raise_on_exception:bool=False,**kwargs):
    """
    Creates a DynamoDB table intended to store locks, with flexible customization options. 
    
    **This function does not handle credentials or access keys, you need to provide an already instantiated boto3.client 
    with your credentials data OR a generic boto3.client will be created using the default boto3 session**.

    This function simplifies the creation of a DynamoDB table by predefining key parameters, but also supports several
    optional configurations via keyword arguments. The default behavior is to create a table with:
    
    - Primary key: 'lock_id' (type 'S')
    - Billing mode: 'PAY_PER_REQUEST'
    - TTL (Time to Live) enabled on attribute 'ttl'
    - Table class: 'STANDARD'

    Optionally, you can provide a custom boto3 DynamoDB client. If not provided, a new client will be created using the
    specified region.
    
    This function does not support the creation of local or global secondary indexes.

    Parameters:
        table_name (str): The name of the DynamoDB table to be created.
        boto3_client (BaseClient): An existing boto3 DynamoDB client. 
        verbose (bool, optional): If True, prints detailed progress and validation messages. Default is True.
        raise_on_exception (bool, optional): If True, raises exceptions instead of returning False on failure. Default is False.
        **kwargs: Additional parameters to customize table creation:
            - key_name (str): Name of the primary key attribute. Default is 'lock_id'.
            - key_type (str): Type of the key attribute. Default is 'S'.
            - billing_mode (str): 'PAY_PER_REQUEST' or 'PROVISIONED'. Default is 'PAY_PER_REQUEST'.
            - table_class (str): 'STANDARD' or 'STANDARD_INFREQUENT_ACCESS'. Default is 'STANDARD'.
            - delete_protection (bool): Enable deletion protection. Default is False.
            - read_capacity_units (int): Provisioned read capacity (if billing_mode is 'PROVISIONED').
            - write_capacity_units (int): Provisioned write capacity (if billing_mode is 'PROVISIONED').
            - max_read_requests (int): Optional On-Demand throughput configuration.
            - max_write_requests (int): Optional On-Demand throughput configuration.
            - read_units_per_second (int): Optional Warm throughput configuration.
            - write_units_per_second (int): Optional Warm throughput configuration.
            - stream_enabled (bool): Enable DynamoDB Streams. Default is False.
            - stream_view_type (str): View type for streams ('NEW_IMAGE', 'OLD_IMAGE', 'NEW_AND_OLD_IMAGES', 'KEYS_ONLY').
            - sse_enabled (bool): Enable server-side encryption. Default is False.
            - sse_type (str): SSE type ('AES256' or 'KMS'). Default is 'AES256'.
            - kms_master_key_id (str): Required if sse_type is 'KMS'. Must be a valid KMS key ID.
            - resource_policy (str): Optional IAM resource policy as a JSON string.
            - tags (list): List of tags in format [{'Key': ..., 'Value': ...}].

    Returns:
    
        bool: True if the table was successfully created (and TTL configured), False if failed and raise_on_exception is False.

    Raises:
    
        Exception: If raise_on_exception is True and any validation or AWS call fails, otherwise will return True or False.

    Example:
        >>> DynamoDBLock.create_dynamodb_table(
        ...     table_name="my_lock_table",
        ...     boto3_client=MyAWSDynamoDBClient,
        ...     billing_mode="PROVISIONED",
        ...     read_capacity_units=5,
        ...     write_capacity_units=5,
        ...     verbose=True,
        ...     raise_on_exception=True
        ... )

    Notes:
        - If you provide a boto3 client, it must be a valid DynamoDB client.
        - To update TTL information may require retries while the table is still being created. Normally the total time does not exceed 10 seconds.
        - For full API details: 
            * boto3 docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/create_table.html
            * AWS docs: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html
    """
    if boto3_client is not None:
        try:
            service_name = boto3_client.meta.service_model.service_name
            aws_region_name = boto3_client.meta.region_name
        except Exception as ERR:
            error_message = f"> The parameter provided in boto3_client does not appear to be a valid AWS client - Error: {str(ERR)}"
            if verbose: print(error_message)
            if raise_on_exception: raise AttributeError(error_message) from None
            return False
        if service_name != 'dynamodb':
            error_message = f"> The provided boto3_client is not a client of the DynamoDB service (expected: dynamodb, current: {service_name})"
            if verbose: print(error_message)
            if raise_on_exception: raise AttributeError(error_message) from None
            return False
    else:
        error_message = f"> Missing boto3_client parameter."
        if verbose: print(error_message)
        if raise_on_exception: raise AttributeError(error_message) from None
        return False

    with ElapsedTimer() as elapsed:
        try:
            DDBTableParams = {}
            DDBTableTTLParams = {'TableName':table_name,'TimeToLiveSpecification':{'Enabled':True,'AttributeName':'ttl'}}
            ##──── Table Name ────────────────────────────────────────────────────────────────────────────────────────────────────────────────
            DDBTableParams.update({'TableName':table_name})
            ##──── Attribute Definitions ─────────────────────────────────────────────────────────────────────────────────────────────────────
            key_name = kwargs.get("key_name","lock_id")
            key_type = kwargs.get("key_type","S")
            DDBTableParams.update({'AttributeDefinitions':[{'AttributeName':key_name,'AttributeType':key_type}]})
            DDBTableParams.update({'KeySchema':[{'AttributeName':'lock_id','KeyType':'HASH'}]})
            ##──── Resource Policy ───────────────────────────────────────────────────────────────────────────────────────────────────────────
            resource_policy = kwargs.get("resource_policy",None)
            if resource_policy is not None and not isinstance(resource_policy,str):
                error_message = f"> Invalid resource_policy parameter: Must be an JSON document converted to string."
                if verbose: print(error_message)
                if raise_on_exception: raise Exception(error_message) from None
                return False
            if resource_policy is not None:
                DDBTableParams.update({'ResourcePolicy':resource_policy})
            ##──── Billing Mode ──────────────────────────────────────────────────────────────────────────────────────────────────────────────
            billing_mode = kwargs.get("billing_mode","PAY_PER_REQUEST").upper()
            if billing_mode not in ["PROVISIONED","PAY_PER_REQUEST"]:
                error_message = f"> Invalid billing_mode parameter: Accepted values are PROVISIONED or PAY_PER_REQUEST."
                if verbose: print(error_message)
                if raise_on_exception: raise Exception(error_message) from None
                return False
            DDBTableParams.update({'BillingMode':billing_mode})
            ##──── Table class ───────────────────────────────────────────────────────────────────────────────────────────────────────────────
            table_class = kwargs.get("table_class","STANDARD").upper()
            if table_class not in ["STANDARD","STANDARD_INFREQUENT_ACCESS"]:
                error_message = f"> Invalid table_class parameter: Accepted values are STANDARD or STANDARD_INFREQUENT_ACCESS."
                if verbose: print(error_message)
                if raise_on_exception: raise Exception(error_message) from None
                return False
            DDBTableParams.update({'TableClass':table_class})
            ##──── Delete Protection ─────────────────────────────────────────────────────────────────────────────────────────────────────────
            delete_protection_enabled = bool(kwargs.get("delete_protection",False))
            if not isinstance(delete_protection_enabled,bool):
                error_message = f"> Invalid delete_protection_enabled parameter: Accepted values are True or False."
                if verbose: print(error_message)
                if raise_on_exception: raise Exception(error_message) from None
                return False
            DDBTableParams.update({'DeletionProtectionEnabled':delete_protection_enabled})
            ##──── Provisioned Throughput ────────────────────────────────────────────────────────────────────────────────────────────────────
            try:
                read_capacity = kwargs.get("read_capacity_units",None)
                write_capacity = kwargs.get("write_capacity_units",None)
            except Exception as ERR:
                error_message = f"> Invalid Provisioned Throughput parameters: Value of read_capacity or write_capacity must be an integer. {str(ERR)}"
                if verbose: print(error_message)
                if raise_on_exception: raise Exception(error_message) from None
                return False
            rc = {'ReadCapacityUnits':int(read_capacity)} if read_capacity is not None and isinstance(read_capacity,int) else {}
            wc = {'WriteCapacityUnits':int(write_capacity)} if write_capacity is not None and isinstance(write_capacity,int) else {}
            if rc != {} or wc != {}: DDBTableParams['ProvisionedThroughput'] = {}
            if rc != {}: DDBTableParams['ProvisionedThroughput'].update(rc)
            if wc != {}: DDBTableParams['ProvisionedThroughput'].update(wc)
            ##──── OnDemand Throughput ───────────────────────────────────────────────────────────────────────────────────────────────────────
            try:
                max_read_requests = kwargs.get("max_read_requests",None)
                max_write_requests = kwargs.get("max_write_requests",None)
            except Exception as ERR:
                error_message = f"> Invalid OnDemand Throughput parameters: Value of max_read_requests or max_write_requests must be an integer. {str(ERR)}"
                if verbose: print(error_message)
                if raise_on_exception: raise Exception(error_message) from None
                return False
            mr = {'MaxReadRequestUnits':int(max_read_requests)} if max_read_requests is not None and isinstance(max_read_requests,int) else {}
            mw = {'MaxWriteRequestUnits':int(max_write_requests)} if max_write_requests is not None and isinstance(max_write_requests,int) else {}
            if mr != {} or mw != {}: DDBTableParams['OnDemandThroughput'] = {}
            if mr != {}: DDBTableParams['OnDemandThroughput'].update(mr)
            if mw != {}: DDBTableParams['OnDemandThroughput'].update(mw)
            ##──── Warm Throughput ───────────────────────────────────────────────────────────────────────────────────────────────────────────
            try:
                read_units_per_second = kwargs.get("read_units_per_second",None)
                write_units_per_second = kwargs.get("write_units_per_second",None)
            except Exception as ERR:
                error_message = f"> Invalid OnDemand Throughput parameters: Value of max_read_requests or max_write_requests must be an integer. {str(ERR)}"
                if verbose: print(error_message)
                if raise_on_exception: raise Exception(error_message) from None
                return False
            ru = {'ReadUnitsPerSecond':int(read_units_per_second)} if read_units_per_second is not None and isinstance(read_units_per_second,int) else {}
            wu = {'WriteUnitsPerSecond':int(write_units_per_second)} if write_units_per_second is not None and isinstance(write_units_per_second,int) else {}
            if ru != {} or wu != {}: DDBTableParams['WarmThroughput'] = {}
            if ru != {}: DDBTableParams['WarmThroughput'].update(ru)
            if wu != {}: DDBTableParams['WarmThroughput'].update(wu)
            ##──── Stream Specification ──────────────────────────────────────────────────────────────────────────────────────────────────────
            stream_enabled = bool(kwargs.get("stream_enabled",True))
            stream_view_type = kwargs.get("stream_view_type","NEW_AND_OLD_IMAGES").upper()
            if stream_enabled:
                if stream_view_type not in ['NEW_IMAGE','OLD_IMAGE','NEW_AND_OLD_IMAGES','KEYS_ONLY']:
                    error_message = f"> Invalid Stream Specification parameters: stream_view_type must be NEW_IMAGE or OLD_IMAGE or NEW_AND_OLD_IMAGES or KEYS_ONLY."
                    if verbose: print(error_message)
                    if raise_on_exception: raise Exception(error_message) from None
                    return False
                DDBTableParams.update({'StreamSpecification':{'StreamEnabled':True,'StreamViewType':stream_view_type}})
            ##──── SSE Specification ─────────────────────────────────────────────────────────────────────────────────────────────────────────
            sse_enabled = bool(kwargs.get("sse_enabled",False))
            sse_type = kwargs.get("sse_type","AES256").upper()
            kms_master_key_id = kwargs.get("kms_master_key_id",None)
            sse_dict = {'SSESpecification':{'Enabled':True}} if sse_enabled else {}
            kms_key_id_pattern = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
            if sse_enabled:
                error_message = ''
                if sse_type == 'AES256':
                    sse_dict['SSESpecification']['SSEType'] = 'AES256'
                elif sse_type == 'KMS':
                    sse_dict['SSESpecification']['SSEType'] = 'KMS'
                    if kms_master_key_id is not None and isinstance(kms_master_key_id,str):
                        if re.match(kms_key_id_pattern,kms_master_key_id):
                            sse_dict['SSESpecification']['KMSMasterKeyId'] = kms_master_key_id
                        else:
                            error_message = f"> Invalid SSE Specification parameters: The kms_master_key_id appears to be invalid (pattern: '{kms_key_id_pattern}' )."
                    else:
                        error_message = f"> Invalid SSE Specification parameters: Missing kms_master_key_id."
                else:
                    error_message = f"> Invalid SSE Specification parameters: sse_type must be AES256 or KMS. If your choose KMS, you need to supply a kms_master_key_id"
                if error_message != '':
                    if verbose: print(error_message)
                    if raise_on_exception: raise Exception(error_message) from None
                    return False
            DDBTableParams.update(sse_dict)
            ##──── Tags ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
            tags = kwargs.get("tags",None)
            if not (tags is None or tags is []):
                error_message = ''
                if not isinstance(tags, list):
                    error_message = "> Invalid tags parameter: the value of tags must be a list of dict like [{'Key':'key_name','Value':'key_value'}]'"
                else:
                    for tag in tags:
                        if not isinstance(tag, dict):
                            error_message = "> Invalid tags parameter: each value in tags list must be a dict like [{'Key':'key_name','Value':'key_value'}]'"
                        elif 'Key' not in tag.keys() or 'Value' not in tag.keys():
                            error_message = "> Invalid tags parameter: each tag must have the keys 'Key' and 'Value' like [{'Key':'key_name','Value':'key_value'}]'"
                        elif not isinstance(tag['Key'], str) or not isinstance(tag['Value'], str):
                            error_message = "> Invalid tags parameter: the keys 'Key' and 'Value' must be strings like [{'Key':'key_name_string','Value':'key_value_string'}]'"
                if error_message != '':
                    if verbose: print(error_message)
                    if raise_on_exception: raise Exception(error_message) from None
                    return False
                DDBTableParams.update({'Tags':tags})
            ##────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
            if verbose: 
                print(f"> Parameters to create table '{table_name}' successfully validated! {elapsed.text()}")
                print(json.dumps(DDBTableParams,indent=3,sort_keys=False,ensure_ascii=False,default=str))
            with ElapsedTimer() as elapsed_create_table:
                if verbose:
                    print(f"> Creating table '{table_name}' in region '{aws_region_name}'.")
                try:
                    response = boto3_client.create_table(**DDBTableParams)
                except Exception as ERR:
                    if not raise_on_exception:
                        if verbose: 
                            print(f"> Failed to create table '{table_name}' {str(ERR)} {elapsed_create_table.text()}")
                        return False
                    raise Exception(str(ERR)) from None

                if response.get("ResponseMetadata",{}).get("HTTPStatusCode",0) == 200:
                    if verbose: 
                        print(f"> Table '{table_name}' created successfully! {elapsed_create_table.text()}")
                        print(f"> Wating 3 seconds before update TTL information on table '{table_name}' ...")
                        time.sleep(3)
                    del response['ResponseMetadata']

                    with ElapsedTimer() as elapsed_ttl:
                        while True:
                            if verbose: 
                                print(f"> Trying to update TTL information on table '{table_name}'...")
                            try:
                                response_ttl = boto3_client.update_time_to_live(**DDBTableTTLParams)
                                if response_ttl.get("ResponseMetadata",{}).get("HTTPStatusCode",0) == 200:
                                    if verbose: 
                                        print(f"> Successfully updated TTL information on table '{table_name}'! {elapsed_ttl.text()}")
                                    del response_ttl['ResponseMetadata']
                                    break
                                else:
                                    if verbose: 
                                        print(f"> Failed to update TTL on table '{table_name}'. Please verify manually. {elapsed_ttl.text()}")
                                    del response_ttl['ResponseMetadata']
                                    break
                            except Exception as ERR:
                                if verbose:
                                    print(f"> Table '{table_name}' still in creating process... wait more 3 seconds...")
                                time.sleep(3)
                    if verbose: 
                        print(json.dumps(response,indent=3,sort_keys=False,ensure_ascii=False,default=str))
                        print(json.dumps(response_ttl,indent=3,sort_keys=False,ensure_ascii=False,default=str))
                    
            if verbose:
                print(f"> All done! {elapsed_ttl.text()}")
            return True
        except Exception as ERR:
            if not raise_on_exception:
                if verbose: 
                    print(f"> Failed at create_dynamodb_table ({table_name}): {str(ERR)} {elapsed.text()}")
                return False
            raise Exception(str(ERR)) from None
    
