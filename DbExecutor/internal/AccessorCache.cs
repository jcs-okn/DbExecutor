using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Codeplex.Data.Internal
{
    internal static class AccessorCache
    {
        static readonly Dictionary<Type, IKeyIndexed<string, IMemberAccessor>>
            cache = new Dictionary<Type, IKeyIndexed<string, IMemberAccessor>>();

        public static IKeyIndexed<string, ExpressionAccessor> Lookup(Type targetType)
        {
            lock (cache)
            {
                IKeyIndexed<string, IMemberAccessor> accessors;
                if (!cache.TryGetValue(targetType, out accessors))
                {
                    var props = targetType.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.GetProperty | BindingFlags.SetProperty)
                        .Select(pi => new ExpressionAccessor(pi));

                    var fields = targetType.GetFields(BindingFlags.Public | BindingFlags.Instance | BindingFlags.GetField | BindingFlags.SetField)
                      .Select(fi => new ExpressionAccessor(fi));

                    accessors = KeyIndexed.Create(props.Concat(fields), a => a.Name, a => a);
                    cache.Add(targetType, accessors);
                };

                return (IKeyIndexed<string, ExpressionAccessor>)accessors;
            }
        }
    }
}