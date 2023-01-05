#include "../common/allocator.h"

#define LFENCE asm volatile("lfence" : : : "memory")
#define SFENCE asm volatile("sfence" : : : "memory")

template <class T>
class Node
{
public:
};

template <class T>
class NonBlockingQueue
{
    CustomAllocator my_allocator_;
public:
    
    NonBlockingQueue() : my_allocator_()
    {
        std::cout << "Using NonBlockingQueue\n";
    }

    void initQueue(long t_my_allocator_size){
        std::cout << "Using Allocator\n";
        my_allocator_.initialize(t_my_allocator_size, sizeof(Node<T>));
        // Initialize the queue head or tail here
        Node<T>* newNode = (Node<T>*)my_allocator_.newNode();
        my_allocator_.freeNode(newNode);
    }

    void enqueue(T value)
    {
        // Use LFENCE and SFENCE as mentioned in pseudocode
    }

    bool dequeue(T *value)
    {
        // Use LFENCE and SFENCE as mentioned in pseudocode
        bool ret_value = false;
        return ret_value;
    }

    void cleanup()
    {
        my_allocator_.cleanup();
    }

};

