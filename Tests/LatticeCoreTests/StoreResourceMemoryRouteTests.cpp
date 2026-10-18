#include <gtest/gtest.h>
#include "../../Sources/LatticeCore/src/ordinary_resource_memory_route.hpp"
#include <string>
using lattice::detail::resource_memory_route::exempt;
TEST(StoreResourceMemoryRoute, ExactAnonymousAndDisposableTemporary){EXPECT_TRUE(exempt(":memory:",false));EXPECT_TRUE(exempt("",false));}
TEST(StoreResourceMemoryRoute, ActualSdkNamedMemoryForms){EXPECT_TRUE(exempt("file:name?mode=memory&cache=shared",true));EXPECT_TRUE(exempt("file:na%3Fme%23%25%20?mode=memory&cache=shared",true));EXPECT_TRUE(exempt("file:?mode=memory&cache=shared",true));}
TEST(StoreResourceMemoryRoute, ActualCoreSharedAnonymousForm){EXPECT_TRUE(exempt("file::memory:?cache=shared",true));EXPECT_TRUE(exempt("file::memory:",true));}
TEST(StoreResourceMemoryRoute, LiteralFileWithoutActualUriFlagCannotUseExemption){EXPECT_FALSE(exempt("file::memory:",false));EXPECT_FALSE(exempt("file:name?mode=memory&cache=shared",false));}
TEST(StoreResourceMemoryRoute, PlainPathSubstringsAreNotMemory){EXPECT_FALSE(exempt("/tmp/notes:memory:.sqlite",true));EXPECT_FALSE(exempt("notes-mode=memory.sqlite",true));}
TEST(StoreResourceMemoryRoute, UriPathSubstringDoesNotBecomeAnonymousMemory){EXPECT_FALSE(exempt("file:/private/notes:memory:.sqlite",true));EXPECT_FALSE(exempt("file:prefix:memory:",true));EXPECT_FALSE(exempt("file:./:memory:",true));}
TEST(StoreResourceMemoryRoute, ModeKeyAndValueMustBeExact){EXPECT_FALSE(exempt("file:db?xmode=memory",true));EXPECT_FALSE(exempt("file:db?mode=memoryevil",true));EXPECT_FALSE(exempt("file:db?mode=rw&cache=shared",true));}
TEST(StoreResourceMemoryRoute, FragmentCannotSupplyIgnoredMode){EXPECT_FALSE(exempt("file:db#?mode=memory",true));EXPECT_FALSE(exempt("file:db?cache=shared#&mode=memory",true));}
TEST(StoreResourceMemoryRoute, DuplicateParametersRefuseAmbiguousPolicy){EXPECT_FALSE(exempt("file:db?mode=memory&mode=memory",true));EXPECT_FALSE(exempt("file:db?mode=memory&cache=shared&cache=shared",true));}
TEST(StoreResourceMemoryRoute, UnknownVfsAndLockPoliciesDoNotGetExemption){EXPECT_FALSE(exempt("file:db?mode=memory&vfs=other",true));EXPECT_FALSE(exempt("file:db?mode=memory&immutable=1",true));EXPECT_FALSE(exempt("file:db?mode=memory&nolock=1",true));}
TEST(StoreResourceMemoryRoute, EmbeddedNulAndInvalidEscapesRefuse){const std::string nul("file:disk\0?mode=memory",22);EXPECT_FALSE(exempt(nul,true));EXPECT_FALSE(exempt("file:a%00b?mode=memory",true));EXPECT_FALSE(exempt("file:a%2?mode=memory",true));}
TEST(StoreResourceMemoryRoute, RealPercentDecodedQueryAndAnonymousName){EXPECT_TRUE(exempt("file:db?m%6fde=mem%6fry&cache=private",true));EXPECT_TRUE(exempt("file:%3Amemory%3A?cache=shared",true));}
