#pragma once
#include <iostream>
#include <cstddef>
#include <mutex>
#include <atomic>
#include <assert.h>
#include "MarkableReference.h"

// Linked list abstract
template<class T>
class ILinkedList {
public:
	// Add an element to the list
	virtual void add(const T &val) = 0;

	// Find an element, return containment
	// Stores found element in val
	virtual bool find(T &val) = 0;
};

namespace ll {

	// Full support lock free ll
	template<class T>
	class LockFreeLL : ILinkedList<T> {
	private:
		std::mutex mtx;

		// Regular Linked-List Node
		class Node {
		public:
			MarkableReference<Node> next;
			T val;
			bool isCap;

			// Keep track of how many people are watching this reference
			std::atomic<int> watchers;

			Node() : isCap(true), watchers(0) {} // Dummy node for head and tail
			Node(T val) : val(val), isCap(false), watchers(0) {}
		};

		// Abstraction layer for lots of watches
		Node *getAndWatch(Node *node) {
			if (node != nullptr)
				node->watchers++;
			return node;
		}
		void stopWatching(Node *node) {
			if (node != nullptr)
				node->watchers--;
		}

		Node *head;
		std::atomic<size_t> curSize;

	public:
		// Construct with dummy head
		LockFreeLL() : curSize(0) {
			// Make head and tail, both caps
			head = new Node();
			head->next = MarkableReference<Node>(new Node());
		}

		// Destruct, free all nodes
		virtual ~LockFreeLL() {
			Node *curr = head;
			while (curr != nullptr) {
				Node *next = curr->next.getRef();
				delete curr;
				curr = next;
			}
		}

		// Returns the head. Not thread safe.
		Node *NOT_THREAD_SAFE_getHead() { return head; }

		void safeDelete(Node *toDelete) {
			while (toDelete->watchers > 0);
			delete toDelete;
		}

		// Find a value, internal use
		// Note that we do not stop watching returned nodes
		// This *must* be cleaned up by the callee
		std::pair<Node *, Node *> _find(const T &val) {
			Node *pred, *curr, *succ;
			bool marked;

			// Allow ourselves to jump back and retry
retry:;

			// Head is guaranteed to exist, dummy node
			pred = getAndWatch(head);
			curr = getAndWatch(pred->next.getRef());

			// While we have yet to reach the end of the list
			while (true) {
				succ = getAndWatch(curr->next.getBoth(marked));

				while (marked) {
					// Try to physically delete the logically deleted node
					Node *expectedRef = curr;
					bool expectedMark = false;
					Node *requiredRef = succ;
					bool requiredMark = false;

					if (!(pred->next.compareExchangeBothWeak(
						expectedRef,
						expectedMark,
						requiredRef,
						requiredMark
					))) {
						stopWatching(pred);
						stopWatching(curr);
						stopWatching(succ);
						goto retry;
					}

					// Move and free mem
					Node *toDelete = curr;

					curr = succ;
					succ = getAndWatch(curr->next.getBoth(marked));

					stopWatching(toDelete);
					safeDelete(toDelete);
				}

				// If we found it, return
				if (curr->isCap || curr->val == val) {
					stopWatching(succ);
					return { pred, curr };
				}

				// Move
				stopWatching(pred);
				pred = curr;
				curr = succ;
			}
		}

		// Add item to list
		void add(const T &val) {
			while (true) {
				// Find our val
				auto [ pred, curr ] = _find(val);

				// Item already exists (don't match with cap)
				if (!curr->isCap && curr->val == val) {
					stopWatching(pred);
					stopWatching(curr);
					return;
				}

				// Can now guarantee that curr is the tail (which is a cap)
				// Attempt to link it in with CAS
				Node *node = new Node(val);
				node->next = MarkableReference<Node>(curr);

				Node *expectedRef = curr;
				bool expectedMark = false;
				Node *requiredRef = node;
				bool requiredMark = false;

				bool success = pred->next.compareExchangeBothWeak(
					expectedRef,
					expectedMark,
					requiredRef,
					requiredMark
				);
				stopWatching(pred);
				stopWatching(curr);

				if (success) {
					curSize++;
					return;
				}
			}
		}

		// Remove item from list
		bool remove(const T &val) {
			while (true) {
				auto [ pred, curr ] = _find(val);

				// We didn't find it, stop
				if (curr->isCap || !(curr->val == val)) {
					stopWatching(pred);
					stopWatching(curr);
					return false;
				}

				// Logically delete node by marking it's successor
				Node *succ = curr->next.getRef();

				Node *expectedRef = succ;
				bool expectedMark = false;
				Node *requiredRef = succ;
				bool requiredMark = true;

				// Logical deletion, might need to retry
				if (!(curr->next.compareExchangeBothWeak(
					expectedRef,
					expectedMark,
					requiredRef,
					requiredMark
				))) {
					stopWatching(pred);
					stopWatching(curr);
					continue;
				}

				// It worked, attempt physical!
				Node *toDelete = curr;

				expectedRef = curr;
				expectedMark = false;
				requiredRef = succ;
				requiredMark = false;

				// If cut was successful, free mem
				// If it doesn't work immediately, don't worry about it
				// _find will clean
				bool wasCut = pred->next.compareExchangeBothWeak(
					expectedRef,
					expectedMark,
					requiredRef,
					requiredMark
				);
				curSize--;

				stopWatching(pred);
				stopWatching(curr);

				// Free ourselves
				if (wasCut)
					safeDelete(toDelete);

				return true;
			}
		}

		// Returns true if the item is in the list,
		// parameter updated
		bool find(T &val) {
			auto [ pred, curr ] = _find(val);

			bool found = false;

			// If we have a real internal node that matches us
			if (curr->val == val && !curr->next.getMark() && !curr->isCap) {
				val = curr->val;
				found = true;
			}

			stopWatching(pred);
			stopWatching(curr);

			return found;
		}

		bool find_all(T &val, std::vector<T> &v) {
			return true;
		}

		// Get current size
		size_t size() { return curSize; }
	};

	// Lock free linked list
	// No support for deletion
	template<class T>
	class AddOnlyLockFreeLL : ILinkedList<T> {
	private:
		// Regular linked list node
		struct Node {
			std::atomic<Node *> next = {nullptr};
			T val;

			Node() {} // Dummy node for head
			Node(T val) : val(val) {}
		};

		// Size and head
		std::atomic_size_t curSize;
		Node *head;

	public:
		// Construct
		AddOnlyLockFreeLL() : curSize(0) {
			head = new Node();
		}

		// Free everything
		virtual ~AddOnlyLockFreeLL() {
			Node *curr = head;
			while (curr != nullptr) {
				Node *toRemove = curr;
				curr = curr->next;
				delete toRemove;
			}
		}

		// Get the head, not thread safe
		Node *NOT_THREAD_SAFE_getHead() { return head; }

		// Add new element to the list
		void add(const T &val) {
			Node *toAdd = new Node(val);

			// Keep going till we find success
			while (true) {
				Node *pred = head, *curr = head->next;

				while (curr != nullptr) {
					// Found it, update
					if (curr->val == val) {
						curr->val = val;
						delete toAdd;
						return;
					}

					pred = curr;
					curr = curr->next;
				}

				// Connect new node
				toAdd->next = curr;

				// Add with CAS
				Node *expected = curr;
				Node *required = toAdd;
				if (pred->next.compare_exchange_weak(
					expected,
					required
				)) {
					curSize++;
					return;
				}
			}
		}

		bool find(T &val) {
			Node *curr = head->next;

			while (curr != nullptr) {
				// Found it
				if (curr->val == val) {
					val = curr->val;
					return true;
				}

				curr = curr->next;
			}

			return false;
		}

		bool find_all(T &val, std::vector<T> &v) {
			Node *curr = head->next;

			while (curr != nullptr) {
				// Found it
				if (curr->val == val) {
					v.push_back(curr->val);
				}

				curr = curr->next;
			}

			return true;
		}

		size_t size() { return curSize; }
	};

	// Hand over hand locked linked list
	template<class T>
	class LockableLL : ILinkedList<T> {
	private:
		// Lockable linked-list node
		struct LockableNode {
		private:
			// Lock
			std::mutex mtx;

		public:
			// Member variables
			LockableNode *next = nullptr;
			T val;

			// Construct
			LockableNode() {} // Dummy node for head
			LockableNode(T val) : val(val) {}

			// Wrappers for thread control
			void lock() { mtx.lock(); }
			void unlock() { mtx.unlock(); }

			// Lock the next node and return it
			LockableNode *getNextAndLock() {
				if (next == nullptr)
					return nullptr;
				next->mtx.lock();
				return next;
			}
		};

		// Member variables
		LockableNode *head = new LockableNode();
		std::atomic_size_t curSize;

	public:
		// Construct a new Linked-List
		LockableLL() : curSize(0) {}

		// Destructor, free all nodes
		virtual ~LockableLL() {
			// Obtain lock on head
			LockableNode *mover = head;
			mover->lock();

			// Free everything
			while (mover != nullptr) {
				LockableNode *next = mover->getNextAndLock();

				mover->unlock();
				delete mover;

				mover = next;
			}
		}

		// Returns the head. Not thread safe
		LockableNode *NOT_THREAD_SAFE_getHead() { return head; }

		// Add new element to the linked list
		void add(const T &val) {
			// Maintain lock on cur node
			LockableNode *mover = head;
			mover->lock();

			// Traverse the list
			bool isHead = true;
			while (true) {
				// If we find it, update
				if (!isHead && mover->val == val) {
					mover->val = val;
					mover->unlock();
					return;
				}

				// Lock our next node (if it exists)
				LockableNode *next = mover->getNextAndLock();
				// Stop if we've reached the end
				if (next == nullptr)
					break;

				// Unlock and move
				mover->unlock();
				mover = next;
				isHead = false;
			}

			// Insert at end
			mover->next = new LockableNode(val);
			mover->unlock();
			curSize++;
		}

		// Remove an element
		// Returns whether or not the value was successfully removed
		bool remove(T val) {
			// Maintain lock on current node
			LockableNode *mover = head;
			mover->lock();

			// Traverse, look for node to remove
			while (true) {
				// Get the next node
				LockableNode *next = mover->getNextAndLock();

				// Break if we're done
				if (next == nullptr) {
					mover->unlock();
					return false;
				}

				// Found it, remove
				if (next->val == val) {
					mover->next = next->next;
					mover->unlock();
					next->unlock();

					delete next;
					curSize--;

					return true;
				}

				// Unlock and move
				mover->unlock();
				mover = next;
			}

			mover->unlock();
			return false;
		}

		// Return existence, store val in param
		bool find(T &val) {
			// Empty list
			if (head->next == nullptr)
				return false;

			// Maintain lock on current node
			LockableNode *mover = head->next;
			mover->lock();

			// Check for existence
			while (true) {
				if (mover->val == val) {
					val = mover->val;
					mover->unlock();
					return true;
				}

				// Get next and break if done
				LockableNode *next = mover->getNextAndLock();

				if (next == nullptr) {
					mover->unlock();
					return false;
				}

				// Move
				mover->unlock();
				mover = next;
			}
		}

		bool find_all(T &val, std::vector<T> &v) {
			return true;
		}

		// Get the current size
		size_t size() { return curSize; }
	};
};