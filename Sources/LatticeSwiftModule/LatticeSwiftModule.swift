import Foundation

public protocol OptionalProtocol {
    associatedtype Wrapped
    
    init()
    var pointee: Wrapped { get set }
    func hasValue() -> Bool
    func value() -> Self.Wrapped
}

extension Optional: OptionalProtocol {
    public init() {
        self.init(nilLiteral: ())
    }
    public var pointee: Wrapped {
        get {
            return self!
        }
        set {
            self = newValue
        }
    }
    public func hasValue() -> Bool {
        return self != nil
    }
    public func value() -> Self.Wrapped {
        return self!
    }
}

//extension UnsafeMutablePointer: OptionalProtocol {
//    public typealias Wrapped = Pointee
//    public init() {
////        self.init(mutating: UnsafePointer<Pointee>.init(OpaquePointer.init(bitPattern: 0)))
//    }
//    
//    public func hasValue() -> Bool {
//        self.pointee != nil
//    }
//    
//    public func value() -> Pointee {
//        self.pointee
//    }
//    
//}
public protocol CxxManagedType {
    associatedtype SwiftType
    associatedtype OptionalType: OptionalProtocol
    
//    associatedtype CxxManagedOptionalType: CxxManagedType, DefaultInitializable where Self.CxxManagedOptionalType.SwiftType: OptionalProtocol
    init()
    func get() -> SwiftType
    @discardableResult mutating func set(_ newValue: SwiftType) -> UnsafeMutablePointer<Self>
//    static func getManagedField(from object: lattice.ManagedModel, with name: String) -> Self
}
