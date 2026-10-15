#ifndef LATTICE_INSTALLATION_CHANNEL_H
#define LATTICE_INSTALLATION_CHANNEL_H
#ifdef __cplusplus
extern "C" {
#endif

// New-namespace installer entry only. Not normal startup or store enrollment.
// The product calls this before its startup side effects, creates exactly its
// primary store in the supplied retained namespace, returns from the callback,
// then exits the process with the returned status. It must not start ordinary
// workers, sync, providers, nested processes, or continue its regular main.
// Descriptors 3 and 4 are owned/closed by the receiver. An arbitrary environment
// variable, path or decoded offer is never an active managed-open capability.
typedef int (*lattice_engram_seed_callback_v1)(const char* exact_path, void* context);
int lattice_receive_engram_seed_v1(lattice_engram_seed_callback_v1 callback, void* context);

#ifdef __cplusplus
}
#endif
#endif
