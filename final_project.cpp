#include <iostream>
#include <algorithm>
#include <fstream>
#include <chrono>
#include <map>
#include <vector>
#include <list>
#include <unordered_map>
#include <string>
#include <memory>
#include <sstream>
#include <limits>
#include <thread>
#include <queue>
#include <optional>
#include <random>
#include <mutex>
#include <shared_mutex>
#include <cassert>
#include <cstring> 
#include <exception>
#include <atomic>

#define UNUSED(p)  ((void)(p))

enum FieldType { INT, FLOAT, STRING };

// Define a basic Field variant class that can hold different types
class Field {
public:
    FieldType type;
    std::unique_ptr<char[]> data;
    size_t data_length;

public:
    Field(int i) : type(INT) { 
        data_length = sizeof(int);
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), &i, data_length);
    }

    Field(float f) : type(FLOAT) { 
        data_length = sizeof(float);
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), &f, data_length);
    }

    Field(const std::string& s) : type(STRING) {
        data_length = s.size() + 1;  // include null-terminator
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), s.c_str(), data_length);
    }

    Field& operator=(const Field& other) {
        if (&other == this) {
            return *this;
        }
        type = other.type;
        data_length = other.data_length;
        std::memcpy(data.get(), other.data.get(), data_length);
        return *this;
    }

    Field(Field&& other){
        type = other.type;
        data_length = other.data_length;
        std::memcpy(data.get(), other.data.get(), data_length);
    }

    FieldType getType() const { return type; }
    int asInt() const { 
        return *reinterpret_cast<int*>(data.get());
    }
    float asFloat() const { 
        return *reinterpret_cast<float*>(data.get());
    }
    std::string asString() const { 
        return std::string(data.get());
    }

    std::string serialize() {
        std::stringstream buffer;
        buffer << type << ' ' << data_length << ' ';
        if (type == STRING) {
            buffer << data.get() << ' ';
        } else if (type == INT) {
            buffer << *reinterpret_cast<int*>(data.get()) << ' ';
        } else if (type == FLOAT) {
            buffer << *reinterpret_cast<float*>(data.get()) << ' ';
        }
        return buffer.str();
    }

    void serialize(std::ofstream& out) {
        std::string serializedData = this->serialize();
        out << serializedData;
    }

    static std::unique_ptr<Field> deserialize(std::istream& in) {
        int type; in >> type;
        size_t length; in >> length;
        if (type == STRING) {
            std::string val; in >> val;
            return std::make_unique<Field>(val);
        } else if (type == INT) {
            int val; in >> val;
            return std::make_unique<Field>(val);
        } else if (type == FLOAT) {
            float val; in >> val;
            return std::make_unique<Field>(val);
        }
        return nullptr;
    }

    void print() const{
        switch(getType()){
            case INT: std::cout << asInt(); break;
            case FLOAT: std::cout << asFloat(); break;
            case STRING: std::cout << asString(); break;
        }
    }
};

class Tuple {
public:
    std::vector<std::unique_ptr<Field>> fields;

    void addField(std::unique_ptr<Field> field) {
        fields.push_back(std::move(field));
    }

    size_t getSize() const {
        size_t size = 0;
        for (const auto& field : fields) {
            size += field->data_length;
        }
        return size;
    }

    std::string serialize() {
        std::stringstream buffer;
        buffer << fields.size() << ' ';
        for (const auto& field : fields) {
            buffer << field->serialize();
        }
        return buffer.str();
    }

    void serialize(std::ofstream& out) {
        std::string serializedData = this->serialize();
        out << serializedData;
    }

    static std::unique_ptr<Tuple> deserialize(std::istream& in) {
        auto tuple = std::make_unique<Tuple>();
        size_t fieldCount; in >> fieldCount;
        for (size_t i = 0; i < fieldCount; ++i) {
            tuple->addField(Field::deserialize(in));
        }
        return tuple;
    }

    void print() const {
        for (const auto& field : fields) {
            field->print();
            std::cout << " ";
        }
        std::cout << "\n";
    }
};

static constexpr size_t PAGE_SIZE = 4096;  // Fixed page size
static constexpr size_t MAX_SLOTS = 512;   // Fixed number of slots
static constexpr size_t MAX_PAGES= 1000;   // Total Number of pages that can be stored
uint16_t INVALID_VALUE = std::numeric_limits<uint16_t>::max(); // Sentinel value

struct Slot {
    bool empty = true;                  // Is the slot empty?    
    uint16_t offset = INVALID_VALUE;    // Offset of the slot within the page
    uint16_t length = INVALID_VALUE;    // Length of the slot
};

// Slotted Page class
class SlottedPage {
public:
    std::unique_ptr<char[]> page_data = std::make_unique<char[]>(PAGE_SIZE);
    size_t metadata_size = sizeof(Slot) * MAX_SLOTS;

    SlottedPage(){
        // Empty page -> initialize slot array inside page
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        for (size_t slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
            slot_array[slot_itr].empty = true;
            slot_array[slot_itr].offset = INVALID_VALUE;
            slot_array[slot_itr].length = INVALID_VALUE;
        }
    }

    /*
    Compacts the SlottedPage structure by moving all non-empty tuples to contiguous memory regions
    This function iterates through all slots in the page, checking if each slot contains a tuple.
    If a tuple's current offset does not match the calculated compacted offset (curr_offset),
    it is moved to the new compacted location. The slot's offset is updated accordingly.
    The function maintains `curr_offset` as the next available position for a tuple within the page.
    */
    void compactPage() {
        size_t curr_offset = metadata_size;
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());

        for (size_t i = 0; i < MAX_SLOTS; i++) {
            if (!slot_array[i].empty) {
                size_t tuple_size = slot_array[i].length;
                size_t old_offset = slot_array[i].offset;

                if (curr_offset != old_offset) {
                    memmove(page_data.get() + curr_offset, page_data.get() + old_offset, tuple_size);
                    slot_array[i].offset = curr_offset;
                }

                curr_offset += tuple_size;
            }
        }
    }
    
    /*
    Searches for an empty slot with sufficient space to hold a tuple of the required size.
    Iterates through the slot array to find an empty slot whose length is at least `requiredSize`.
    If a suitable slot is found, its index is returned. If no suitable slot is found, returns -1.
    */
    int findFreeSlot(size_t requiredSize) {
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        for (size_t i = 0; i < MAX_SLOTS; ++i) {
            if (slot_array[i].empty && slot_array[i].length >= requiredSize) {
                return i; // Return the index of the suitable slot
            }
        }
        return -1; // No suitable slot found
    }

    /*
    Calculates and sets the offset for a specified slot based on its size and the page structure.
    If the slot has not been assigned an offset (indicated by INVALID_VALUE), it calculates the offset 
    based on the previous slot's position and length. For the first slot, it assigns the offset directly
    after the metadata. If there is insufficient space to fit the tuple within the page, marks the slot as empty
    and returns false. Returns true if offset setting was successful.
    */
    bool calculateAndSetOffset(int slotIndex, size_t tupleSize) {
        size_t offset = INVALID_VALUE;
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        if (slot_array[slotIndex].offset == INVALID_VALUE) {
            if (slotIndex != 0) {
                auto prev_slot_offset = slot_array[slotIndex - 1].offset;
                auto prev_slot_length = slot_array[slotIndex - 1].length;
                offset = prev_slot_offset + prev_slot_length;
            } else {
                offset = metadata_size;
            }
            slot_array[slotIndex].offset = offset;
        } else {
            offset = slot_array[slotIndex].offset;
        }

        // Check if the tuple fits in the remaining page space
        if (offset + tupleSize >= PAGE_SIZE) {
            slot_array[slotIndex].empty = true;
            slot_array[slotIndex].offset = INVALID_VALUE;
            return false;  // Not enough space
        }

        return true;
    }

    /*
    Updates the tuple at the specified slot index with new data, relocating it if necessary.
    If the slot has enough space to hold the updated tuple, it overwrites the existing data.
    Otherwise, finds a new empty slot with sufficient space (using `findFreeSlot`). If a new slot is found,
    calculates and sets the new offset, invalidates the old slot, and copies the data to the new slot.
    Returns true on success, and false if there is insufficient space.
    */
    bool updateTuple(int slotIndex, std::unique_ptr<Tuple> tuple) {
        auto serializedTuple = tuple->serialize();
        size_t tuple_size = serializedTuple.size();

        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        if (slot_array[slotIndex].length >= tuple_size) {
            std::memcpy(page_data.get() + slot_array[slotIndex].offset, 
                    serializedTuple.c_str(), 
                    tuple_size);
            return true;
        } 

        int new_slot_index = findFreeSlot(tuple_size);
        if (new_slot_index == -1) {
            return false;
        } else {
            bool setNewOffset = calculateAndSetOffset(new_slot_index, tuple_size);
            
            if (!setNewOffset) {
                return false;
            } else {
                // invalidate current slot index
                slot_array[slotIndex].empty = true;
                slot_array[slotIndex].offset = INVALID_VALUE;
                slot_array[slotIndex].length = INVALID_VALUE;

                // move tuple to new slot
                slot_array[new_slot_index].empty = false;
                slot_array[new_slot_index].length = tuple_size;
                std::memcpy(page_data.get() + slot_array[new_slot_index].offset, 
                    serializedTuple.c_str(), 
                    tuple_size);
                    return true;
            }
        }
    }   

    // Add a tuple, returns true if it fits, false otherwise.
    bool addTuple(std::unique_ptr<Tuple> tuple) {

        // Serialize the tuple into a char array
        auto serializedTuple = tuple->serialize();
        size_t tuple_size = serializedTuple.size();

        // Check for first slot with enough space
        int slot_itr = findFreeSlot(tuple_size);
        
        if (slot_itr == -1){
            std::cout << "Page does not contain an empty slot with sufficient space to store the tuple.";
            return false;
        }

        // Identify the offset where the tuple will be placed in the page
        // Update slot meta-data if needed
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        slot_array[slot_itr].empty = false;
        bool result = calculateAndSetOffset(slot_itr, tuple_size);

        if (result == false){
            return false;
        } else {
            slot_array[slot_itr].length = tuple_size;
            std::memcpy(page_data.get() + slot_array[slot_itr].offset, 
                    serializedTuple.c_str(), 
                    tuple_size);
            return true;
        }

        // Copy serialized data into the page
        

        return true;
    }

    void deleteTuple(size_t index) {
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        size_t slot_itr = 0;
        for (; slot_itr < MAX_SLOTS; slot_itr++) {
            if(slot_itr == index &&
               slot_array[slot_itr].empty == false){
                slot_array[slot_itr].empty = true;
                break;
               }
        }

        //std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    void print() const{
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        for (size_t slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
            if (slot_array[slot_itr].empty == false){
                assert(slot_array[slot_itr].offset != INVALID_VALUE);
                const char* tuple_data = page_data.get() + slot_array[slot_itr].offset;
                std::istringstream iss(tuple_data);
                auto loadedTuple = Tuple::deserialize(iss);
                std::cout << "Slot " << slot_itr << " : [";
                std::cout << (uint16_t)(slot_array[slot_itr].offset) << "] :: ";
                loadedTuple->print();
            }
        }
        std::cout << "\n";
    }
};

const std::string database_filename = "buzzdb.dat";

class StorageManager {
public:    
    std::fstream fileStream;
    size_t num_pages = 0;
    std::mutex io_mutex;

public:
    StorageManager(){
        fileStream.open(database_filename, std::ios::in | std::ios::out);
        if (!fileStream) {
            // If file does not exist, create it
            fileStream.clear(); // Reset the state
            fileStream.open(database_filename, std::ios::out);
        }
        fileStream.close(); 
        fileStream.open(database_filename, std::ios::in | std::ios::out); 

        fileStream.seekg(0, std::ios::end);
        num_pages = fileStream.tellg() / PAGE_SIZE;

        // std::cout << "Storage Manager :: Num pages: " << num_pages << "\n";        
        if(num_pages == 0){
            extend();
        }
    }

    ~StorageManager() {
        if (fileStream.is_open()) {
            fileStream.close();
        }
    }

    // Read a page from disk
    std::unique_ptr<SlottedPage> load(uint16_t page_id) {
        std::lock_guard<std::mutex>  io_guard(io_mutex);
        assert(page_id < num_pages);
        
        fileStream.seekg(page_id * PAGE_SIZE, std::ios::beg);
        auto page = std::make_unique<SlottedPage>();
        // Read the content of the file into the page
        if(fileStream.read(page->page_data.get(), PAGE_SIZE)){
            // std::cout << "Page read successfully from file."<< page_id<< std::endl;
        }
        else{
            std::cerr << "Error: Unable to read data from the file :: page id "<<page_id<<" \n";
            exit(-1);
        }
        return page;
    }

    // Write a page to disk
    void flush(uint16_t page_id, const std::unique_ptr<SlottedPage>& page) {
        std::lock_guard<std::mutex>  io_guard(io_mutex);
        size_t page_offset = page_id * PAGE_SIZE;        

        // Move the write pointer
        fileStream.seekp(page_offset, std::ios::beg);
        fileStream.write(page->page_data.get(), PAGE_SIZE);        
        fileStream.flush();
    }

    // Extend database file by one page
    void extend() {
        std::lock_guard<std::mutex>  io_guard(io_mutex);

        // Create a slotted page
        auto empty_slotted_page = std::make_unique<SlottedPage>();

        // Move the write pointer
        fileStream.seekp(0, std::ios::end);

        // Write the page to the file, extending it
        fileStream.write(empty_slotted_page->page_data.get(), PAGE_SIZE);
        fileStream.flush();

        // Update number of pages
        num_pages += 1;
    }

    void extend(uint64_t till_page_id) {
        std::lock_guard<std::mutex>  io_guard(io_mutex); 
        uint64_t write_size = std::max(static_cast<uint64_t>(0), till_page_id + 1 - num_pages) * PAGE_SIZE;
        if(write_size > 0 ) {
            std::cout << "Extending database file till page id : "<<till_page_id<<" \n";
            char* buffer = new char[write_size];
            std::memset(buffer, 0, write_size);

            fileStream.seekp(0, std::ios::end);
            fileStream.write(buffer, write_size);
            fileStream.flush();
            
            num_pages = till_page_id+1;
        }
    }

    /*
    * Finds a page in the storage that has enough space for a tuple of the given size.
    * This function iterates through all pages and checks if any page has a free slot 
    * large enough to accommodate the tuple. If such a page is found, its page ID is returned.
    * If no page has enough space, -1 is returned.
    */
    int findPageWithSpace(size_t tuple_size) {
        for (size_t page_id = 0; page_id < num_pages; page_id++) {
            auto page = load(page_id);
            if (page->findFreeSlot(tuple_size) != -1) {
                return page_id;
            }
        }
        return -1;
    }

    /*
    * Adds a tuple to the storage, possibly across multiple pages, if necessary. 
    * This function first serializes the tuple and tries to add it to the pages one by one. 
    * If a tuple is successfully added to a page, the page is flushed to disk and the function returns true. 
    * If there is not enough space on any of the existing pages, the function extends the storage 
    * by adding a new page, and then attempts to add the tuple to the new page. 
    * If the tuple cannot be added, the function returns false.
    */
    bool addTupleAcrossPage(std::unique_ptr<Tuple> tuple) {
        std::string serialized_tuple = tuple->serialize(); // Serialize the tuple

        for (size_t page_id = 0; page_id < num_pages; ++page_id) {
            auto page = load(page_id);
            
            // Create an input stream from the serialized string for deserialization
            std::istringstream serialized_tuple_stream(serialized_tuple);

            // Reconstruct the tuple
            auto reconstructed_tuple = Tuple::deserialize(serialized_tuple_stream);

            if (page->addTuple(std::move(reconstructed_tuple))) {  // Use the reconstructed tuple
                flush(page_id, page);
                return true;
            }
        }

        // Extend and retry
        extend();
        size_t new_page_id = num_pages - 1;
        auto new_page = load(new_page_id);

        // Reconstruct again for the new page
        std::istringstream serialized_tuple_stream(serialized_tuple);
        auto reconstructed_tuple = Tuple::deserialize(serialized_tuple_stream);

        if (new_page->addTuple(std::move(reconstructed_tuple))) {
            flush(new_page_id, new_page);
            return true;
        }

        return false;  // Failed to add tuple
    }



};

using PageID = uint64_t;
using FrameID = uint64_t;

class Policy {
public:
    virtual bool touch(PageID page_id) = 0;
    virtual PageID evict() = 0;
    virtual ~Policy() = default;

    friend class BufferManager;
};

void printList(std::string list_name, const std::list<PageID>& myList) {
        std::cout << list_name << " :: ";
        for (const PageID& value : myList) {
            std::cout << value << ' ';
        }
        std::cout << '\n';
}

constexpr size_t MAX_PAGES_IN_MEMORY = 10;

class BufferFrame {
private:
    friend class BufferManager;
    
    // TODO: Add necessary member variables, e.g., page ID, frame ID, flags for dirty and exclusive status, etc.
    PageID pageId; // identifier for data on disk
    FrameID frameId; // identifier for data in memory
    bool dirty;
    bool exclusive;
    int use_counter;
    std::shared_mutex lock;
    bool valid;

public:
    std::unique_ptr<SlottedPage> page;  // The actual page data in memory

    // TODO: Add constructor(s) and any necessary methods here to manage a BufferFrame object.
    // HINT: Think about how you will manage the page data, dirty status, and locking mechanisms (if needed).
    BufferFrame() : valid(false) { };

    BufferFrame(PageID pid, FrameID fid) : pageId(pid), frameId(fid), dirty(false), exclusive(false), use_counter(0), valid(true) { };

    void setDirty(bool val) {
        dirty = val;
    }

    bool getDirty() {
        return dirty;
    }

    void incrementCounter() {
        use_counter += 1;
    }
    
    void decrementCounter() {
        use_counter = std::max(use_counter - 1, 0);
    }
    
    int getCounter() {
        return use_counter;
    }

    PageID getPageId() {
        return pageId;
    }

    FrameID getFrameId() {
        return frameId;
    }

    void sharedLock() {
        lock.lock_shared();
    }

    void sharedUnlock() {
        lock.unlock_shared();
    }

    void exclusiveLock() {
        lock.lock_shared();
        exclusive = true;
    }

    void exclusiveUnlock() {
        lock.lock_shared();
        exclusive = false;
    }

    bool isExclusiveLock() {
        return exclusive;
    }

    bool getValid() {
        return valid;
    }

    void setValid(bool val) {
        valid = val;
    }
};

class buffer_full_error : public std::exception {
    public:
        const char *what() const noexcept override { return "buffer is full"; }
};

class BufferManager {
private:
    StorageManager storage_manager;      // Responsible for I/O operations (reading and writing pages to disk)
    std::vector<std::unique_ptr<BufferFrame>> buffer_pool;  // The buffer pool containing loaded pages
    
    // TODO: Add your implementation here for page locks, FIFO/LRU queues, and page use counters.
    // HINT: Consider the role of synchronization and locking for thread safety.
    std::list<PageID> lru_list;
    std::list<PageID> fifo_list;

    std::mutex buffer_pool_mutex;
    std::list<FrameID> available_frames;

    std::unordered_map<PageID, FrameID> map;

public:
    uint64_t capacity_;  // Number of pages that can be stored in memory at once
    uint64_t page_size_; // The size of each page in bytes

    /// Constructor
    BufferManager() {
        capacity_ = MAX_PAGES_IN_MEMORY;
        // TODO: Preallocate buffer frames, locks, and other necessary structures.
        // HINT: Ensure that you handle the initialization of page metadata and ensure thread safety.
        buffer_pool.reserve(capacity_);
        for (uint64_t i = 0; i < capacity_; i++) {
            buffer_pool.push_back(std::make_unique<BufferFrame>());
            available_frames.push_back(i);
        }

        storage_manager.extend(MAX_PAGES);  // Extend storage for disk pages
    }

    /// Flushes a specific page to disk.
    void flushPage(FrameID frame_id) {
        // TODO: Implement logic to write the page data to disk if it's dirty.
        // HINT: Use the `StorageManager` to perform disk operations.
        std::lock_guard<std::mutex> buffer_pool_lock(buffer_pool_mutex);
        
        auto frame = buffer_pool[frame_id].get();
        if (frame->getDirty() && frame->getCounter() == 0) {
            frame->exclusiveLock();
            storage_manager.flush(frame->getPageId(), frame->page);
            frame->setDirty(false);
            frame->exclusiveUnlock();
        }
    }

    /// Destructor. Ensures that all dirty pages are flushed to disk.
    ~BufferManager() {
        // TODO: Iterate through the buffer pool and flush any dirty pages to disk.
        // HINT: Consider thread safety if there are multiple threads unfixing pages concurrently.
        for (auto& frame : buffer_pool) {
            if (frame->getDirty() && frame->getCounter() == 0 && !frame->isExclusiveLock()) {
                frame->exclusiveLock();
                storage_manager.flush(frame->getPageId(), frame->page);
                frame->setDirty(false);
                frame->exclusiveUnlock();
            }
        }
    }

    PageID evict() {
    
        for (auto it = fifo_list.begin(); it != fifo_list.end(); ++it) {
            FrameID fid = map[*it];
            BufferFrame& frame = *(buffer_pool[fid].get());
            
            if (frame.getCounter() == 0) {
                frame.exclusiveLock();
                PageID evictedPageId = frame.getPageId();
                if (frame.getDirty()) {
                    storage_manager.flush(evictedPageId, frame.page);
                }
                map.erase(map.find(evictedPageId));
                buffer_pool[fid]->setValid(false);
                fifo_list.erase(it);
                available_frames.push_back(fid);
                frame.exclusiveUnlock();
                return evictedPageId;
            }
            
        }

        for (auto it = lru_list.begin(); it != lru_list.end(); ++it) {
            FrameID fid = map[*it];
            auto frame = buffer_pool[fid].get();
            if (frame->getCounter() == 0) {
                PageID evictedPageId = frame->getPageId();
                if (frame->getDirty()) {
                    storage_manager.flush(evictedPageId, frame->page);
                }
                map.erase(map.find(evictedPageId));
                buffer_pool[fid]->setValid(false);
                lru_list.erase(it);
                available_frames.push_back(fid);
                return evictedPageId;
            }
        }
        
        throw buffer_full_error();
    }

    /// Fixes a page in memory (loads it if not already present) and returns a reference to it.
    /// Is thread-safe w.r.t. other concurrent calls to `fix_page()` and
    /// `unfix_page()`.     
    /// @param[in] page_id   The ID of the page to be fixed (loaded into memory).
    /// @param[in] exclusive Whether the page should be locked exclusively (for writing).
    /// @return Reference to the BufferFrame object for the fixed page.
    BufferFrame& fix_page(PageID page_id, bool exclusive) {
        // TODO: Implement logic to load the page if it's not already in memory.
        // HINT: Handle eviction if the buffer is full and synchronize access for thread safety.
        std::lock_guard<std::mutex> buffer_pool_lock(buffer_pool_mutex);

        for (auto& frame : buffer_pool) {
            if (frame->getValid() && frame->getPageId() == page_id) {
                

                auto it = std::find(lru_list.begin(), lru_list.end(), page_id);

                if (it != lru_list.end()) {
                    lru_list.erase(it);
                    lru_list.push_back(page_id);
                } else {
                    auto it = std::find(fifo_list.begin(), fifo_list.end(), page_id);
                    fifo_list.erase(it);
                    lru_list.push_back(page_id);
                }

                if (exclusive) {
                    std::cout<<""<<std::endl;
                    frame->exclusiveLock();
                } else {
                    frame->sharedLock();
                }
                frame->incrementCounter();
                return *frame;
            }
        }

        if (lru_list.size() + fifo_list.size() >= capacity_) {
            evict();
        }
       
        auto page = storage_manager.load(page_id);
        FrameID frame_id = available_frames.front();
        
        available_frames.pop_front();
        auto frame = std::make_unique<BufferFrame>(page_id, frame_id);
        frame->page = std::move(page);
        map[page_id] = frame_id;

        if (exclusive) {
            frame->exclusiveLock();
        } else {
            frame->sharedLock();
        }
        
        frame->incrementCounter();
        fifo_list.push_back(page_id);
        
        buffer_pool[frame_id] = std::move(frame);
        buffer_pool[frame_id]->setValid(true);
        return *buffer_pool[frame_id];
    }

    /// Unfixes a page, marking it as no longer in use. If `is_dirty` is true, the page will eventually be written to disk.
    /// @param[in] page   Reference to the BufferFrame object representing the page.
    /// @param[in] is_dirty  If true, marks the page as dirty (modified).
    void unfix_page(BufferFrame& page, bool is_dirty) {
        // TODO: Implement logic to release the lock on the page and mark it as dirty if necessary.
        // HINT: Consider both exclusive and shared locks, and synchronize appropriately.
        std::lock_guard<std::mutex> buffer_pool_lock(buffer_pool_mutex);
        // if (page.isExclusiveLock()) {
        //     uint64_t& value = *reinterpret_cast<uint64_t*>(page.page->page_data.get());
        //     std::cout<<"val unfix"<<value<<std::endl;
        // }

        if (is_dirty) {
            page.setDirty(true);
        }
        page.decrementCounter();

        if (page.isExclusiveLock()) {
            page.exclusiveUnlock();
        } else {
            page.sharedUnlock();
        }
    }


    void extend(){
        storage_manager.extend();
    }
    
    size_t getNumPages(){
        return storage_manager.num_pages;
    }

    /// Returns the page IDs of all pages in the FIFO list, in FIFO order.
    /// Is not thread-safe.
    std::vector<PageID> get_fifo_list() const {
        // TODO: add your implementation here
        std::vector<PageID> page_ids;
        page_ids.reserve(fifo_list.size());
        for (const auto& pid : fifo_list) {
            page_ids.push_back(pid);
        }
        return page_ids;
    }

    /// Returns the page IDs of all pages in the LRU list, in LRU order.
    /// Not thread-safe.
    /// Is not thread-safe.
    std::vector<PageID> get_lru_list() const {
        // TODO: add your implementation here
        std::vector<PageID> page_ids;
        page_ids.reserve(lru_list.size());
        for (const auto& pid : lru_list) {
            page_ids.push_back(pid);
        }
        return page_ids;
    }

};

class BuzzDB {
public:
    BufferManager buffer_manager;

public:
    BuzzDB(){
        // Storage Manager automatically created
    }
};

int main(int argc, char* argv[]) {    
    bool execute_all = false;
    std::string selected_test = "-1";

    if(argc < 2) {
        execute_all = true;
    } else {
        selected_test = argv[1];
    }


    if (execute_all || selected_test == "1") {
        std::cout << "...Starting Test 1: Tuple Movement and Compaction within a Single Page" << std::endl;

        auto page = std::make_unique<SlottedPage>();

        // Step 1: Add multiple tuples to the page
        for (int i = 0; i < 5; ++i) {
            // Create a Tuple with a single Field containing the string representation of 'i'
            auto field = std::make_unique<Field>(std::to_string(i));
            auto tuple = std::make_unique<Tuple>();
            tuple->addField(std::move(field));  // Add the field to the tuple

            assert(page->addTuple(std::move(tuple)) == true);  // Ensure the tuple was added successfully
        }

        // Step 2: Delete some tuples
        page->deleteTuple(1); // Delete the second tuple
        page->deleteTuple(3); // Delete the fourth tuple

        // Step 3: Compact the page
        page->compactPage();

        // Step 4: Validate that the page contains no gaps
        Slot* slot_array = reinterpret_cast<Slot*>(page->page_data.get());
        size_t expected_offset = page->metadata_size;

        // Loop through the slots to check that the offsets and lengths are valid after compaction
        for (size_t i = 0; i < MAX_SLOTS; ++i) {
            if (!slot_array[i].empty) {
                assert(slot_array[i].offset == expected_offset); // Ensure no gaps in the page
                expected_offset += slot_array[i].length; // Move to the next expected offset
            }
        }

        std::cout << "Passed: Test 1" << std::endl;
    }

    return 0;
}