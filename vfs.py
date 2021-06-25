import threading
import time
import math

# This class will be used to represent blocks and data partitioned as blocks
class BlockDevice:
  def __init__(self, block_size, num_blocks):
    #set block_size
    self.block_size = block_size
    #This represents the total memory available as blocks
    self.data = bytearray(block_size * num_blocks)

  # This method is used for reading the data from blocks
  def readblocks(self, block_num, buf, offset=0):
    addr = block_num * self.block_size + offset
    for i in range(len(buf)):
      buf[i] = self.data[addr + i]

  # This method is used for writing data to the blocks
  def writeblocks(self, block_num, buf, offset=None):
    if offset is None:
      # do erase, then write
      for i in range(len(buf) // self.block_size):
        self.ioctl(6, block_num + i)
      offset = 0
    addr = block_num * self.block_size + offset
    for i in range(len(buf)):
      self.data[addr + i] = buf[i]

  # standard function part of block device
  def ioctl(self, op, args):
    # block count
    if op == 4:
      return len(self.data) // self.block_size
    # block size
    if op == 5:
      return self.block_size
    # block erase
    if op == 6:
      return 0

# This class will be used to create nodes for linkedlist
class Node:
  def __init__(self, val, next=None, prev=None):
    self.val = val
    self.next = next
    self.prev = prev

# This is a standard implementation of singly linkedlist
class LinkedList:
  def __init__(self):
    self.head = None
    self.tail = None

  #This method is used to add new nodes to the linked list
  def add(self, node:Node):
    if self.head is None:
      self.head = node
      self.tail = node
    else:
      self.tail.next = node
      self.tail = self.tail.next

  #This method is used to remove the head from the linkedlist
  def removeHead(self):
    if not self.head:
        return None
    temp = self.head

    # Move the head pointer to the next node
    self.head = self.head.next
    return temp

#This class is used to store the name length and block address corresponding to a file
class INode:
  def __init__(self,file_name):
    self.file_name = file_name
    self.length = 0
    # Using linkedlist to store blocks to provide linear access to data blocks even if blocks are fregmented
    self.blocks = LinkedList()

# This class is a logical representation of a file and stored metadata like name, size, INode, created updated timestamps
class MemFile:
  def __init__(self, name: str, is_dir: bool = True):
    self.name = name # this is the name of the node (file/directory)
    self.is_dir = is_dir # this flag is used to identify whether current node is a file or a directory
    self.lock = threading.Lock() # this object is used to lock the file when it is accessed by a thread
    self.size = 0 # size of file in bytes
    self.iNode = INode(name) # this object is refernce to the INode object
    self.created_at = time.time()
    self.updated_at = time.time()
    self.children = {} # this map stores reference to child files/directories of current directory

# This is the main File System Class
class MemFS:
  def __init__(self, block_size, num_blocks):
    self.lock = threading.Lock()
    self.root = MemFile("/", True) # by default a root directory is added
    self.__BLOCK_SIZE = block_size # this is the block size defined for the system
    self.__BLOCKS = num_blocks # this is the number of blocks that the system has
    self.blockDevice = BlockDevice(block_size, num_blocks) # reference to the BlockDEvice interface
    self.block_usage_map = [False]*num_blocks # this array can be used to get the count of used and unused blocks,
    self.unused_blocks = LinkedList() # Using a linked list to store unused blocks so we can get unused blocks in O(1) time
    self.unused_blocks_count = num_blocks
    self.__init_block_tracking()

  # This private method is a helper funciton to split the input path string
  def __split(self, path):
    if path == '/':
      return []
    return path.split('/')[1:]

  # this private method is used to find a node from root
  def __getNode(self, path):
    #find node
    curr = self.root
    for s in self.__split(path):
      curr = curr.children[s]
    return curr

  # this private method is used to add a new node(file/folder)
  def __putNode(self, path):
    """
    making the node in this path
    """
    #start from root
    curr = self.root
    for s in self.__split(path):
      #if it doesn't exit, make nodes
      if s not in curr.children:
        #make node
        curr.children[s] = MemFile(s)
      #put node in children
      curr = curr.children[s]
    return curr

  # this private method is used to initilise unused_blocks linkedlist
  def __init_block_tracking(self):
    for i in range(self.__BLOCKS):
      block = Node(i)
      self.unused_blocks.add(block)

  # this method is used to get Unused blocks
  def getUnusedBlock(self):
    self.lock.acquire()
    temp = self.unused_blocks.removeHead()
    self.unused_blocks_count -= 1
    self.block_usage_map[temp.val] = True
    # self.used_blocks.add(Node(temp.val))
    self.lock.release()
    return temp

  # this method is used to add a free block to unused block pool
  def addUnusedBlock(self, block_node):
    self.lock.acquire()
    self.unused_blocks.add(Node(block_node.val))
    self.block_usage_map[block_node.val] = False
    self.unused_blocks_count+=1
    self.lock.release()

  # this function returns a list of children for a given directory path
  def ls(self, path):
    curr = self.__getNode(path)
    #file or directory?
    if not curr.is_dir:
      #return its name
      return [self.__split(path)[-1]]
    #children are sorted and given
    return sorted(curr.children.keys())

  # this method is used to create a directory or an empty file
  def create(self, path, is_dir=False):
    file = self.__putNode(path)
    file.is_dir = is_dir

  # this method is used to open a direcotry or file
  def fopen(self, path):
    file = self.__getNode(path)
    # check if the file is already in use
    if file.lock.locked():
      raise Exception('Cannot open this file since it is already in use/opened')
    # if nobody is using the file acquire lock and return a reference
    file.lock.acquire()
    return file

  # this method is used to release the file/directory
  def fclose(self, file:MemFile):
    file.lock.release()

  # this methof is used to rename the file/directory
  def rename(self, file: MemFile, new_name):
    file.name = new_name
    file.iNode.file_name = new_name
    file.updated_at = time.time()

  # this method is used to write the content of a file
  def fwrite(self, file: MemFile, data: bytearray):
    if file.is_dir:
      raise Exception('Cannot write to a directory')
    #calculate required blocks
    req_blocks = math.ceil(len(data) / self.__BLOCK_SIZE)
    # check if there is space available
    if req_blocks > self.unused_blocks_count:
      overflow = req_blocks - self.unused_blocks_count
      raise Exception('Out of storage, need {} more blocks of memory'.format(overflow))
    #temp buffer
    buf = None
    # variable to keep a track of file size after each iteration
    size = 0
    for i in range(req_blocks):
      # if it's the last block then read only till the end of input data
      if i == req_blocks -1:
        buf = data[i*self.__BLOCK_SIZE:]
      else:
        buf = data[i*self.__BLOCK_SIZE:self.__BLOCK_SIZE]
      # increment the curr file size
      size+= len(buf)
      # get an unused block
      block = self.getUnusedBlock()
      # register block address in INode of the file
      file.iNode.blocks.add(Node(block.val))
      # write the data in the actual data block
      self.blockDevice.writeblocks(block.val,buf)
    # update the final size of the file
    file.size = size
    file.iNode.length = size
    # set update_at timestamp
    file.updated_at = time.time()

  # this method is used to read the content of a file
  def fread(self, file: MemFile):
    if file.is_dir:
      raise Exception('Cannot read directory.')
    # byte array to store the file content to be read from memory blocks
    buf = bytearray()
    # reference to the starting block of the file
    _head = file.iNode.blocks.head
    # reference to the size of file, this will be used to determine the size of temp buffer
    _size = file.size
    # variable to keep track of how many bytes have been read so far
    read_len = 0
    # while there is a next block address available
    while _head :
      # temp buffer
      temp = None
      # is the pending bytes to be read is greater than BLOCK_SIZE then we initialise temp buffer to the size of BLOCK_SIZE
      if _size - read_len > self.__BLOCK_SIZE:
        temp = bytearray(self.__BLOCK_SIZE)
      else:
        # temp buffer size is set to the remaining bytes
        temp = bytearray(_size - read_len)
      #this method fills the temp buffer with the data from data blocks
      self.blockDevice.readblocks(_head.val,temp)
      # update the bytes read so far
      read_len += len(temp)
      # put data in the buffer object which will be returned at the end
      buf.extend(temp)
      # move to the next block
      _head = _head.next
    # return buffer
    return buf

  # def remove(self, file:MemFile, recurr= False):
  #   if file.is_dir and len(file.childs)>0 and not recurr:
  #     raise Exception('Cannot remove this directory as there are {} immediate files/fdirectories inside.'.format(len(file.childs)))
  #   # if it is a file
  #   if file.is_dir == False:
  #     _head = file.iNode.blocks.head
  #     while _head:
  #       self.addUnusedBlock(_head)
  #       _head = _head.next

  #   else:
  #     # if it is a directory
  #     for child in file.childs:
  #       self.remove(child, recurr)
  #       del file.childs[]

# creating file writing and reading single threaded
def testcase1():
  FS = MemFS(10,4)
  FS.create("/ankit/file1.txt")
  _file1 = FS.fopen("/ankit/file1.txt")
  FS.fwrite(_file1,str.encode("file 1 data"))
  print(FS.fread(_file1))
  FS.fclose(_file1)

# creating two files writing and reading multi threaded
def testcase2():
  FS = MemFS(10,4)
  def task1():
    FS.create("/ankit/file1.txt")
    _file1 = FS.fopen("/ankit/file1.txt")
    FS.fwrite(_file1,str.encode("file 1 data is "))
    print(FS.fread(_file1))
    FS.fclose(_file1)

  def task2():
    FS.create("/ankit/file2.txt")
    _file2 = FS.fopen("/ankit/file2.txt")
    FS.fwrite(_file2,str.encode("file2 data"))
    print(FS.fread(_file2))
    FS.fclose(_file2)

  FS = MemFS(10,4)

  t1 = threading.Thread(target=task1, name='t1')
  t2 = threading.Thread(target=task2, name='t2')


  # starting threads
  t1.start()
  t2.start()

  # wait until all threads finish
  t1.join()
  t2.join()

# creating three files writing and reading multi threaded, in this scenario memory overflow happens and exception is raised
def testcase3():
  def task1():
      FS.create("/ankit/file1.txt")
      _file1 = FS.fopen("/ankit/file1.txt")
      FS.fwrite(_file1,str.encode("file 1 data"))
      print(FS.fread(_file1))
      FS.fclose(_file1)

  def task2():
      FS.create("/ankit/file2.txt")
      _file2 = FS.fopen("/ankit/file2.txt")
      FS.fwrite(_file2,str.encode("file"))
      print(FS.fread(_file2))
      FS.fclose(_file2)

  def task3():
      FS.create("/ankit/file3.txt")
      _file2 = FS.fopen("/ankit/file3.txt")
      FS.fwrite(_file2,str.encode("file overflow"))
      print(FS.fread(_file2))
      FS.fclose(_file2)

  FS = MemFS(10,4)

  t1 = threading.Thread(target=task1, name='t1')
  t2 = threading.Thread(target=task2, name='t2')
  t3 = threading.Thread(target=task3, name='t3')
  # starting threads
  t1.start()
  t2.start()
  t3.start()
  # wait until all threads finish
  t1.join()
  t2.join()
  t3.join()


if __name__ == "__main__":

  # creating file writing and reading single threaded
  testcase1()
  # creating two files writing and reading multi threaded
  testcase2()
  # creating three files writing and reading multi threaded, in this scenario memory overflow happens and exception is raised
  testcase3()