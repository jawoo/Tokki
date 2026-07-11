package org.cgiar.tokki;

// Java utilities
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Minimal JSON Schema validator covering the keyword subset used by
 * {@code unit-information.schema.json}: type, required, properties,
 * additionalProperties(false), items, minItems, minimum, maximum,
 * exclusiveMinimum, pattern, and enum.
 *
 * Both the schema and the instance are the plain Map/List/Number/String/Boolean
 * trees produced by SnakeYAML, so the same schema file drives validation here
 * and in prep/csv_to_jsonl.py — no hand-coded rules to drift out of sync.
 */
final class SchemaValidator
{
    private final Map<String, Object> schema;

    SchemaValidator(Map<String, Object> schema)
    {
        this.schema = schema;
    }

    /** Returns an empty list when {@code instance} is valid. */
    List<String> validate(Object instance)
    {
        List<String> errors = new ArrayList<>();
        validate(instance, schema, "", errors);
        return errors;
    }

    @SuppressWarnings("unchecked")
    private void validate(Object node, Map<String, Object> sch, String path, List<String> errors)
    {
        String where = path.isEmpty() ? "(root)" : path;

        // type
        Object type = sch.get("type");
        if (type instanceof String t && !checkType(node, t))
        {
            errors.add(where + ": expected " + t + " but got " + typeName(node));
            return; // further checks are unreliable once the type is wrong
        }

        // enum
        if (sch.get("enum") instanceof List<?> allowed && !allowed.contains(node))
            errors.add(where + ": " + node + " is not one of " + allowed);

        // string constraints
        if (node instanceof String s && sch.get("pattern") instanceof String pat
                && !Pattern.compile(pat).matcher(s).find())
            errors.add(where + ": '" + s + "' does not match pattern " + pat);

        // number constraints
        if (node instanceof Number num)
        {
            double v = num.doubleValue();
            if (sch.get("minimum") instanceof Number m && v < m.doubleValue())
                errors.add(where + ": " + v + " < minimum " + m);
            if (sch.get("maximum") instanceof Number m && v > m.doubleValue())
                errors.add(where + ": " + v + " > maximum " + m);
            if (sch.get("exclusiveMinimum") instanceof Number m && v <= m.doubleValue())
                errors.add(where + ": " + v + " <= exclusiveMinimum " + m);
        }

        // object constraints
        if (node instanceof Map<?, ?> obj)
        {
            Map<String, Object> objMap = (Map<String, Object>) obj;
            if (sch.get("required") instanceof List<?> required)
                for (Object key : required)
                    if (!objMap.containsKey(key))
                        errors.add(where + ": missing required property '" + key + "'");

            Map<String, Object> props = (Map<String, Object>) sch.get("properties");
            if (Boolean.FALSE.equals(sch.get("additionalProperties")) && props != null)
                for (String key : objMap.keySet())
                    if (!props.containsKey(key))
                        errors.add(where + ": unexpected property '" + key + "'");

            if (props != null)
                for (Map.Entry<String, Object> e : objMap.entrySet())
                {
                    Object childSchema = props.get(e.getKey());
                    if (childSchema instanceof Map)
                        validate(e.getValue(), (Map<String, Object>) childSchema,
                                path.isEmpty() ? e.getKey() : path + "." + e.getKey(), errors);
                }
        }

        // array constraints
        if (node instanceof List<?> arr)
        {
            if (sch.get("minItems") instanceof Number m && arr.size() < m.intValue())
                errors.add(where + ": array length " + arr.size() + " < minItems " + m);
            if (sch.get("items") instanceof Map<?, ?> items)
                for (int i = 0; i < arr.size(); i++)
                    validate(arr.get(i), (Map<String, Object>) items, path + "[" + i + "]", errors);
        }
    }

    private static boolean checkType(Object node, String type)
    {
        return switch (type)
        {
            case "object"  -> node instanceof Map;
            case "array"   -> node instanceof List;
            case "string"  -> node instanceof String;
            case "boolean" -> node instanceof Boolean;
            case "null"    -> node == null;
            case "number"  -> node instanceof Number;
            // JSON Schema "integer": any number with no fractional part.
            case "integer" -> node instanceof Number n
                    && !Double.isInfinite(n.doubleValue())
                    && n.doubleValue() == Math.floor(n.doubleValue());
            default        -> true; // unknown type keyword: don't reject
        };
    }

    private static String typeName(Object node)
    {
        if (node == null) return "null";
        if (node instanceof Map) return "object";
        if (node instanceof List) return "array";
        if (node instanceof String) return "string";
        if (node instanceof Boolean) return "boolean";
        if (node instanceof Number) return "number";
        return node.getClass().getSimpleName();
    }
}
