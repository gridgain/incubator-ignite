using Apache.Ignite.Core.Binary;
using System;

namespace Apache.Ignite.Examples.Services.Interop
{
    class DotnetToJavaNameMapper : IBinaryNameMapper
    {
        public string GetFieldName(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            return Char.ToLowerInvariant(name[0]) + name.Substring(1);
        }

        public string GetTypeName(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            var type = Type.GetType(name);

            return type.Namespace == null ? type.FullName : string.Concat(type.Namespace.ToLowerInvariant(), ".", type.Name);
        }
    }
}
