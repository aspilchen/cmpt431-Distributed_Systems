#include "../common/allocator.h"
#include <mutex>

template <class T>
class Node
{
    T value;
    Node<T>* next;
};


template <class T>
class TwoLockQueue
{
    using NodeT = Node<T>;
    std::mutex h_lock;
    std::mutex t_lock;
    NodeT* head;
    NodeT* tail;
    CustomAllocator my_allocator_;

    NodeT* newNode(T val) {
        NodeT* n = static_cast<NodeT*>(my_allocator_.newNode());
        n->value = val;
        n->next = NULL;
    }

public:
    TwoLockQueue() : my_allocator_(), head{NULL}, tail{NULL}
    {
        std::cout << "Using TwoLockQueue\n";
    }

    void initQueue(long t_my_allocator_size){
        std::cout << "Using Allocator\n";
        my_allocator_.initialize(t_my_allocator_size, sizeof(Node<T>));
    }

    void enqueue(T value)
    {
        NodeT* n = newNode(value);

        mtx_e.lock();
        if(head == NULL)
            head = n;
        else
            tail->next = n;
        tail = n;
        mtx_e.unlock();
    }

    bool dequeue(T *value)
    {
        NodeT* n;

        mtx_d.lock();
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