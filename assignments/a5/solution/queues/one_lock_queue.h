#include "../common/allocator.h"
#include <mutex>

template <class T>
class Node
{
public:
    T value;
    Node<T>* next;
};

template <class T>
class OneLockQueue
{
    using NodeT = Node<T>;
    NodeT* head;
    NodeT* tail;
    CustomAllocator my_allocator_;
    std::mutex mtx;
public:
    OneLockQueue() : my_allocator_(), head{NULL}, tail{NULL}
    {
        std::cout << "Using OneLockQueue\n";
    }

    void initQueue(long t_my_allocator_size){
        std::cout << "Using Allocator\n";
        my_allocator_.initialize(t_my_allocator_size, sizeof(NodeT));
        // Initialize the queue head or tail here
        //head = (Node<T>*)my_allocator_.newNode();
        //head->next = NULL;
    }

    void enqueue(T value)
    {
        NodeT* n = static_cast<NodeT*>(my_allocator_.newNode());
        n->value = value;
        n->next = NULL;
        mtx.lock();
        if(head == NULL)
            head = n;
        else
            tail->next = n;
        tail = n;
        mtx.unlock();
    }

    bool dequeue(T *value)
    {
        NodeT* n;

        mtx.lock();
        n = head;
        if(n == NULL){
            mtx.unlock();
            return false;
        }
        head = n->next;
        *value = n->value;
        mtx.unlock();

        my_allocator_.freeNode(n);
        return true;
    }

    void cleanup()
    {
        my_allocator_.cleanup();
    }
};