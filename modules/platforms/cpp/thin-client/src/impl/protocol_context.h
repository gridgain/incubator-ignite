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

#ifndef _IGNITE_IMPL_THIN_PROTOCOL_CONTEXT
#define _IGNITE_IMPL_THIN_PROTOCOL_CONTEXT

#include <stdint.h>

#include <bitset>

#include "impl/protocol_version.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /**
             * Bitmask feature.
             */
            struct BitmaskFeature
            {
                enum Type
                {
                    /** Additional SqlFieldsQuery properties: partitions, updateBatchSize. */
                    QRY_PARTITIONS_BATCH_SIZE = 7,

                    /** Max number of supported bitmask features. */
                    MAX_SUPPORTED
                };
            };

            /**
             * Version feature.
             */
            struct VersionFeature
            {
                class Type
                {
                public:
                    /**
                     * Constructor.
                     *
                     * @param ver Version for the feature;
                     */
                    explicit Type(const ProtocolVersion& ver) :
                        ver(ver)
                    {
                        // No-op.
                    }

                    /**
                     * Get version.
                     *
                     * @return Version.
                     */
                    const ProtocolVersion& GetVersion() const
                    {
                        return ver;
                    }

                private:
                    /** Version. */
                    const ProtocolVersion ver;
                };

                /** Partition awareness. */
                static Type PARTITION_AWARENESS;

                /** Bitmap features. */
                static Type BITMAP_FEATURES;
            };

            /** Protocol context. */
            class ProtocolContext
            {
            public:
                /**
                 * Constructor.
                 *
                 * Constructs a protocol context with specified version and all features set.
                 *
                 * @param ver Version part.
                 */
                explicit ProtocolContext(const ProtocolVersion& ver) :
                    ver(ver),
                    features()
                {
                    if (IsFeatureSupported(VersionFeature::BITMAP_FEATURES))
                        features.set(BitmaskFeature::QRY_PARTITIONS_BATCH_SIZE);
                }

                /**
                 * Get protocol version.
                 *
                 * @return Protocol version.
                 */
                const ProtocolVersion& GetVersion() const
                {
                    return ver;
                }

                /**
                 * Get features bitmask
                 *
                 * @return Feature bitmask.
                 */
                const std::bitset<BitmaskFeature::MAX_SUPPORTED>& GetFeatures() const
                {
                    return features;
                }

                /**
                 * Check if the feature supported.
                 *
                 * @param feature Version feature to check.
                 * @return @true if the feature is supported.
                 */
                bool IsFeatureSupported(const VersionFeature::Type& feature) const
                {
                    return ver >= feature.GetVersion();
                }

                /**
                 * Check if the feature supported.
                 *
                 * @param feature Bitmask feature to check.
                 * @return @true if the feature is supported.
                 */
                bool IsFeatureSupported(BitmaskFeature::Type feature) const
                {
                    if (feature >= BitmaskFeature::MAX_SUPPORTED)
                        return false;

                    return features.test(feature);
                }

            private:
                /** Protocol version. */
                const ProtocolVersion ver;

                /** Features mask. */
                std::bitset<BitmaskFeature::MAX_SUPPORTED> features;
            };
        }
    }
}

#endif //_IGNITE_IMPL_THIN_PROTOCOL_CONTEXT