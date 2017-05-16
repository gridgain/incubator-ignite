/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _IGNITE_ODBC_TEST_COMPLEX_TYPE
#define _IGNITE_ODBC_TEST_COMPLEX_TYPE

#include <string>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

namespace ignite
{
    struct TestObject
    {
        TestObject() :
            f1(412),
            f2("Lorem ipsum")
        {
            // No-op.
        }

        int32_t f1;
        std::string f2;
    };

    struct ComplexType
    {
        ComplexType() :
            i32Field(0)
        {
            // No-op.
        }

        int32_t i32Field;
        TestObject objField;
        std::string strField;
    };
}

namespace ignite
{
    namespace binary
    {

        IGNITE_BINARY_TYPE_START(ignite::TestObject)

            typedef ignite::TestObject TestObject;

            IGNITE_BINARY_GET_TYPE_ID_AS_HASH(TestObject)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(TestObject)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_GET_HASH_CODE_ZERO(TestObject)
            IGNITE_BINARY_IS_NULL_FALSE(TestObject)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(TestObject)

            void Write(BinaryWriter& writer, TestObject obj)
            {
                writer.WriteInt32("f1", obj.f1);
                writer.WriteString("f2", obj.f2);
            }

            TestObject Read(BinaryReader& reader)
            {
                TestObject obj;

                obj.f1 = reader.ReadInt32("f1");
                obj.f2 = reader.ReadString("f2");

                return obj;
            }

        IGNITE_BINARY_TYPE_END

        IGNITE_BINARY_TYPE_START(ignite::ComplexType)

            typedef ignite::ComplexType ComplexType;

            IGNITE_BINARY_GET_TYPE_ID_AS_HASH(ComplexType)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(ComplexType)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_GET_HASH_CODE_ZERO(ComplexType)
            IGNITE_BINARY_IS_NULL_FALSE(ComplexType)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(ComplexType)

            void Write(BinaryWriter& writer, ComplexType obj)
            {
                writer.WriteInt32("i32Field", obj.i32Field);
                writer.WriteObject("objField", obj.objField);
                writer.WriteString("strField", obj.strField);
            }

            ComplexType Read(BinaryReader& reader)
            {
                ComplexType obj;

                obj.i32Field = reader.ReadInt32("i32Field");
                obj.objField = reader.ReadObject<TestObject>("objField");
                obj.strField = reader.ReadString("strField");

                return obj;
            }

        IGNITE_BINARY_TYPE_END
    }
};

#endif // _IGNITE_ODBC_TEST_COMPLEX_TYPE
