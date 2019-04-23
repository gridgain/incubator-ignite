/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @file
 * Declares ignite::binary::BinaryWriter class.
 */

#ifndef _IGNITE_BINARY_BINARY_ENUM_ENTRY
#define _IGNITE_BINARY_BINARY_ENUM_ENTRY

#include <string>
#include <stdint.h>

#include <ignite/common/common.h>

#include "ignite/binary/binary_raw_writer.h"

namespace ignite
{
    namespace binary 
    {
        /**
         * Binary enum entry.
         * 
         * Represents a single entry of enum in a binary form.
         */
        class IGNITE_IMPORT_EXPORT BinaryEnumEntry
        {
        public:
            /**
             * Default constructor.
             */
            BinaryEnumEntry() :
                typeId(0),
                ordinal(0)
            {
                // No-op.
            }

            /**
             * Constructor.
             *
             * @param typeId Type ID of the enum.
             * @param ordinal Ordinal of the enum value.
             */
            BinaryEnumEntry(int32_t typeId, int32_t ordinal) :
                typeId(typeId),
                ordinal(ordinal)
            {
                // No-op.
            }

            /**
             * Get type ID.
             *
             * @return Type ID.
             */
            int32_t GetTypeId() const
            {
                return typeId;
            }

            /**
             * Get ordinal of the enum value.
             *
             * @return Ordinal.
             */
            int32_t GetOrdinal() const
            {
                return ordinal;
            }

        private:
            /** Type ID. */
            int32_t typeId;

            /** Ordinal value. */
            int32_t ordinal;
        };

        }
    }
}

#endif //_IGNITE_BINARY_BINARY_ENUM_ENTRY
