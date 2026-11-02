import LatticeSwiftModule
import LatticeSwiftCppBridge

// The Core header names this SDK protocol in an unrelated object annotation.
// A marker suffices for this isolated module-import test; no background API is
// stubbed here. The actual SDK supplies its full object protocol.
public protocol CxxObject {}

// Type-check real consuming calls, not just the PCM. A valid C++ declaration
// can still import as unavailable when its result is nested in a hidden owner.
func checkBackgroundSyncImport(_ ref: lattice.swift_lattice_ref) {
    let cancellation = lattice.swift_sync_cancellation()
    cancellation.cancel()
    _ = cancellation.is_cancelled()
    let receipt = ref.resume_background_sync(cancellation, true)
    switch receipt.admission {
    case .started, .already_running, .not_owner, .cancelled, .closed, .unsupported: break
    default: break
    }
    let status = ref.get_background_sync_status(receipt.generation)
    switch status.phase {
    case .inactive, .running, .work_complete, .failed, .cancelled: break
    default: break
    }
    _ = ref.cancel_background_sync(receipt.generation)
    _ = ref.suspend_background_sync(receipt.generation)
    _ = ref.current_background_sync_generation()
    _ = ref.is_background_sync_managed()
}
