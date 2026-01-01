import LatticeCpp
import LatticeSwiftCppBridge
import Testing
import Foundation

fileprivate let base64 = Array<UInt8>(
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".utf8
)
/// Generate a random string of base64 filename safe characters.
///
/// - Parameters:
///   - length: The number of characters in the returned string.
/// - Returns: A random string of length `length`.
fileprivate func createRandomString(length: Int) -> String {
  return String(
    decoding: (0..<length).map{
      _ in base64[Int.random(in: 0..<64)]
    },
    as: UTF8.self
  )
}

extension String {
    static func random(length: Int) -> String {
        createRandomString(length: length)
    }
}

class BaseTest {
    deinit {
        paths.forEach { path in
//            try? Lattice.delete(for: .init(fileURL: $0))
        }
    }
    
    private var paths: [URL] = []
    
    func testLattice(isolation: isolated (any Actor)? = #isolation,
                     path: String? = nil,
//                     _ types: any Model.Type...
    ) throws -> lattice.swift_lattice {
        let path = FileManager.default.temporaryDirectory.appending(path: path ?? "\(String.random(length: 32)).sqlite")
        paths.append(path)
        return lattice.swift_lattice(std.string(path.path()))
    }
}

@Suite("Lattice Tests")
class LatticeSwiftCppTests: BaseTest {
    struct Trip {}

    @Test
    func example() async throws {
        let lattice = try testLattice()

    }

    @Test
    func testGetRefForLattice() async throws {
        // Create a swift_lattice_ref via factory
        let config = lattice.configuration()
        let schemas = lattice.SchemaVector()
        let ref1 = lattice.swift_lattice_ref.create(config, schemas)!

        // Get the underlying swift_lattice
        let db = ref1.get()!

        // Add an object to get a managed object
        var obj = lattice.swift_dynamic_object()
        obj.table_name = std.string("TestTable")
        let managed = db.add(consuming: obj)

        // Get the ref back from the managed object's lattice_ pointer
        let ref2 = managed.lattice_ref()
        #expect(ref2 != nil)

        // Both refs should point to the same underlying swift_lattice (compare via hash)
        // This verifies the ptr_cache lookup finds the same cached instance
        #expect(ref1.hash_value() == ref2!.hash_value())
    }

    @Test
    func testLatticeCacheReturnsSharedInstance() async throws {
        // Create two refs with the same config - should get same underlying swift_lattice
        let config = lattice.configuration()
        let schemas = lattice.SchemaVector()

        let ref1 = lattice.swift_lattice_ref.create(config, schemas)!
        let ref2 = lattice.swift_lattice_ref.create(config, schemas)!

        // Both should share the same underlying swift_lattice due to caching
        #expect(ref1.hash_value() == ref2.hash_value())

        // Get the underlying db and verify it's the same pointer
        let db1 = ref1.get()!
        let db2 = ref2.get()!

        // Add an object through db1
        var obj = lattice.swift_dynamic_object()
        obj.table_name = std.string("CacheTest")
        obj.set_string(std.string("name"), std.string("test"))
        let managed = db1.add(consuming: obj)

        // The lookup from managed should work and point to same instance
        let refFromManaged = managed.lattice_ref()
        #expect(refFromManaged != nil)
        #expect(refFromManaged!.hash_value() == ref1.hash_value())

        // Count through db2 should see the object added through db1
        // (since they share the same underlying connection)
        let count = db2.count(std.string("CacheTest"))
        #expect(count == 1)
    }
}
