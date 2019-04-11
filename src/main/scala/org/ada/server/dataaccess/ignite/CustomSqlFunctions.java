package org.ada.server.dataaccess.ignite;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;

public class CustomSqlFunctions {

    @QuerySqlFunction
    public static boolean binEquals(BinaryObject binaryObject, Object value) {
        return safeEquals(binaryObject.deserialize(), value);
    }

    @QuerySqlFunction
    public static boolean binStringEquals(BinaryObject binaryObject, String value) {
        Object object = binaryObject.deserialize();
        return safeEquals((object != null) ? object.toString() : object, value);
    }

    @QuerySqlFunction
    public static boolean binNotEquals(BinaryObject binaryObject, Object value) {
        return !binEquals(binaryObject, value);
    }

    @QuerySqlFunction
    public static boolean binStringNotEquals(BinaryObject binaryObject, String value) {
        return !binStringEquals(binaryObject, value);
    }

    @QuerySqlFunction
    public static boolean binIn(BinaryObject binaryObject, Object ... values) {
        Object object = binaryObject.deserialize();
        for (Object value: values) {
            if (safeEquals(object, value))
                return true;
        }
        return false;
    }

    @QuerySqlFunction
    public static boolean binStringIn(BinaryObject binaryObject, String ... values) {
        Object object = binaryObject.deserialize();
        for (Object value: values) {
            if (safeEquals((object != null) ? object.toString() : object, value))
                return true;
        }
        return false;
    }

    @QuerySqlFunction
    public static boolean binNotIn(BinaryObject binaryObject, Object ... values) {
        return !binIn(binaryObject, values);
    }

    @QuerySqlFunction
    public static boolean binStringNotIn(BinaryObject binaryObject, String ... values) {
        return !binStringIn(binaryObject, values);
    }

    private static boolean safeEquals(Object obj1, Object obj2) {
        return (obj1 == null && obj2 == null) || (obj1 != null && obj2 != null && obj1.equals(obj2));
    }
}