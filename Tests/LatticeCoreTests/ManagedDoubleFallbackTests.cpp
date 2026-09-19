#include <gtest/gtest.h>
#include <LatticeCore.hpp>

using namespace lattice;

TEST(ManagedDoubleFallback, DefaultUnboundValueIsZero) {
    managed<double> value;
    EXPECT_DOUBLE_EQ(value.detach(), 0.0);
}

TEST(ManagedDoubleFallback, MissingAndDeletedRowsReturnZero) {
    database db(":memory:");
    db.execute("CREATE TABLE DoubleFallback(id INTEGER PRIMARY KEY,value)");
    managed<double> value;
    value.assign(&db, nullptr, "DoubleFallback", "value", 1);
    EXPECT_DOUBLE_EQ(value.detach(), 0.0);
    db.execute("INSERT INTO DoubleFallback VALUES(1, 3.5)");
    EXPECT_DOUBLE_EQ(value.detach(), 3.5);
    db.execute("DELETE FROM DoubleFallback WHERE id = 1");
    EXPECT_DOUBLE_EQ(value.detach(), 0.0);
}

TEST(ManagedDoubleFallback, NullAndWrongStoredTypesReturnZero) {
    database db(":memory:");
    db.execute("CREATE TABLE DoubleFallback(id INTEGER PRIMARY KEY,value)");
    db.execute("INSERT INTO DoubleFallback VALUES(1, NULL)");
    managed<double> value;
    value.assign(&db, nullptr, "DoubleFallback", "value", 1);
    EXPECT_DOUBLE_EQ(value.detach(), 0.0);
    db.execute("UPDATE DoubleFallback SET value = 'wrong type'");
    EXPECT_DOUBLE_EQ(value.detach(), 0.0);
    db.execute("UPDATE DoubleFallback SET value = 7");
    EXPECT_DOUBLE_EQ(value.detach(), 0.0);
}

TEST(ManagedDoubleFallback, ExplicitAssignedFallbackSurvivesBindingAndClose) {
    database db(":memory:");
    db.execute("CREATE TABLE DoubleFallback(id INTEGER PRIMARY KEY,value)");
    managed<double> value(9.25);
    value.assign(&db, nullptr, "DoubleFallback", "value", 1);
    EXPECT_DOUBLE_EQ(value.detach(), 9.25);
    db.execute("INSERT INTO DoubleFallback VALUES(1, 3.5)");
    value = 12.75;
    db.execute("UPDATE DoubleFallback SET value = NULL");
    EXPECT_DOUBLE_EQ(value.detach(), 12.75);
    db.close();
    EXPECT_DOUBLE_EQ(value.detach(), 12.75);
}
