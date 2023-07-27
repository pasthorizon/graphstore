//
// Created by per on 23.12.20.
//

#ifndef LIVE_GRAPH_TWO_TRANSACTIONMANAGER_H
#define LIVE_GRAPH_TWO_TRANSACTIONMANAGER_H


#include <atomic>
#include <mutex>
#include <set>
#include <thread>
#include <unordered_map>

#include "Transaction.h"
#include "SnapshotTransaction.h"

#define MIN_VERSION_UPDATER_INTERVAL 100 // The interval in which the minimal version is updated, in microseconds.

class TransactionManager {

public:
    /**
     * Creates a transaction manager.
     * @param max_threads the maximal number of threads used.
     */
    explicit TransactionManager(uint max_threads);
    ~TransactionManager();

    void reset_max_threads(uint max_threads);

    /**
     * Registers a thread with the transaction manager.
     * @param id the id to use out of the dense domain of 0 to max_threads
     * @throws IllegalOperation if the thread id is already taken.
     */
    void register_thread(size_t id);

    /**
     * Deregisters a thread with the transaction manager.
     * @param id the id to use out of the dense domain of 0 to max_threads
     * @throws IllegalOperation if the thread id is not used taken.
     */
    void deregister_thread(size_t id);

    // TODO rename to fit naming convention.
    SnapshotTransaction getSnapshotTransaction(VersionedTopologyInterface* ti, bool write_only);
    void getSnapshotTransaction(VersionedTopologyInterface* ti, bool write_only, SnapshotTransaction& existing_transaction_object);

    version_t draw_timestamp(bool commit_timestamp);

    void transactionCompleted(const Transaction& transaction);

    version_t getMinActiveVersion();
    void update_min_version();

    /**
     * @return a list of all active transaction in descending order. Can contain a sequence of NO_TRANSACTION markers first.
     */
    const vector<version_t>& get_sorted_versions();

    bool create_epoch(uint64_t version);

    version_t get_epoch() const;

private:
    uint max_threads;
    static thread_local size_t thread_id;
    mutex thread_registry_lock;
    vector<bool> thread_id_in_use;

    vector<version_t> active_snapshots;
    atomic<version_t> version {1};
    version_t min_version { numeric_limits<version_t>::min() };
    vector<version_t> sorted_versions;

    thread min_version_updater;
    atomic<bool> stopped;

    version_t epoch_number;

    void run_min_version_updater(uint interval);

    void update_sorted_versions();
};


#endif //LIVE_GRAPH_TWO_TRANSACTIONMANAGER_H
