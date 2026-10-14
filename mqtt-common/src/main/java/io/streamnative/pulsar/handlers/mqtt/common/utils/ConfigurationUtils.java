/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.mqtt.common.utils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import com.google.common.base.Joiner;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.configuration.PulsarConfiguration;

/**
 * Configuration Utils.
 */
public final class ConfigurationUtils {

    public static final String PROTOCOL_NAME = "mqtt";
    public static final String PROTOCOL_PROXY_NAME = "mqtt-proxy";
    public static final String PLAINTEXT_PREFIX = "mqtt://";
    public static final String PROXY_PREFIX = "mqtt-proxy://";
    public static final String SSL_PREFIX = "mqtt+ssl://";
    public static final String SSL_PSK_PREFIX = "mqtt+ssl+psk://";
    public static final String WS_PLAINTEXT_PREFIX = "ws://";
    public static final String WS_SSL_PREFIX = "ws+ssl://";
    public static final String LISTENER_DEL = ",";
    public static final String COLON = ":";
    public static final String LISTENER_PATTERN =
            "^((mqtt)(\\+ssl)?(\\+psk)?|(ws)(\\+ssl)?)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-0-9+]";

    public static final String PROXY_LISTENER_PATTERN =
            "^(mqtt-proxy)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-0-9+]";

    /**
     * Creates PulsarConfiguration and loads it with populated attribute values loaded from provided property file.
     *
     * @param configFile
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public static <T extends PulsarConfiguration> T create(
            String configFile,
            Class<? extends PulsarConfiguration> clazz) throws IOException, IllegalArgumentException {
        checkNotNull(configFile);
        return create(new FileInputStream(configFile), clazz);
    }

    /**
     * Creates PulsarConfiguration and loads it with populated attribute values loaded from provided inputstream
     * property file.
     *
     * @param inStream
     * @throws IOException
     *             if an error occurred when reading from the input stream.
     * @throws IllegalArgumentException
     *             if the input stream contains incorrect value type
     */
    public static <T extends PulsarConfiguration> T create(
            InputStream inStream,
            Class<? extends PulsarConfiguration> clazz) throws IOException, IllegalArgumentException {
        try {
            checkNotNull(inStream);
            Properties properties = new Properties();
            properties.load(inStream);
            return (create(properties, clazz));
        } finally {
            if (inStream != null) {
                inStream.close();
            }
        }
    }

    /**
     * Creates PulsarConfiguration and loads it with populated attribute values from provided Properties object.
     *
     * @param properties The properties to populate the attributed from
     * @throws IOException
     * @throws IllegalArgumentException
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <T extends PulsarConfiguration> T create(
            Properties properties,
            Class<? extends PulsarConfiguration> clazz) throws IOException, IllegalArgumentException {
        checkNotNull(properties);
        T configuration = null;
        try {
            configuration = (T) clazz.newInstance();
            configuration.setProperties(properties);
            update((Map) properties, configuration);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IllegalArgumentException("Failed to instantiate " + clazz.getName(), e);
        }
        return configuration;
    }


    /**
     * Update given Object attribute by reading it from provided map properties.
     *
     * @param properties
     *            which key-value pair of properties to assign those values to given object
     * @param obj
     *            object which needs to be updated
     * @throws IllegalArgumentException
     *             if the properties key-value contains incorrect value type
     */
    public static <T> void update(Map<String, String> properties, T obj) throws IllegalArgumentException {
        Field[] fields = obj.getClass().getDeclaredFields();
        // common fields
        Field[] commonFields = obj.getClass().getSuperclass().getDeclaredFields();
        // super fields
        Field[] superFields = obj.getClass().getSuperclass().getSuperclass().getDeclaredFields();

        Stream.of(fields, commonFields, superFields).flatMap(Stream::of).forEach(f -> {
            if (properties.containsKey(f.getName())) {
                try {
                    f.setAccessible(true);
                    String v = (String) properties.get(f.getName());
                    if (!StringUtils.isBlank(v)) {
                        f.set(obj, value(v.trim(), f));
                    } else {
                        setEmptyValue(v, f, obj);
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException(format("failed to initialize %s field while setting value %s",
                        f.getName(), properties.get(f.getName())), e);
                }
            }
        });
    }

    private static Object value(String value, Field field) {
        checkNotNull(field);
        Type fieldType = field.getGenericType();
        if (fieldType instanceof ParameterizedType) {
            Type[] actualTypes = ((ParameterizedType) fieldType).getActualTypeArguments();
            if (field.getType().equals(List.class)) {
                return stringToList(value, getRawClass(actualTypes[0], field));
            } else if (field.getType().equals(Set.class)) {
                return stringToSet(value, getRawClass(actualTypes[0], field));
            } else if (field.getType().equals(Map.class)) {
                return stringToMap(value, getRawClass(actualTypes[0], field), getRawClass(actualTypes[1], field));
            } else if (field.getType().equals(Optional.class)) {
                Type typeClazz = actualTypes[0];
                if (typeClazz instanceof ParameterizedType) {
                    throw new IllegalArgumentException(format("unsupported non-primitive Optional<%s> for %s",
                            typeClazz.getClass(), field.getName()));
                }
                return Optional.ofNullable(convert(value, getRawClass(typeClazz, field)));
            }
            throw new IllegalArgumentException(format("unsupported field-type %s for %s",
                    field.getType(), field.getName()));
        }
        return convert(value, field.getType());
    }

    private static <T> void setEmptyValue(String value, Field field, T obj)
            throws IllegalArgumentException, IllegalAccessException {
        checkNotNull(field);
        Type fieldType = field.getGenericType();
        if (fieldType instanceof ParameterizedType) {
            if (field.getType().equals(List.class)) {
                field.set(obj, new ArrayList<>());
            } else if (field.getType().equals(Set.class)) {
                field.set(obj, new LinkedHashSet<>());
            } else if (field.getType().equals(Optional.class)) {
                field.set(obj, Optional.empty());
            } else {
                throw new IllegalArgumentException(format("unsupported field-type %s for %s",
                        field.getType(), field.getName()));
            }
        } else if (Number.class.isAssignableFrom(field.getType())) {
            field.set(obj, null);
        }
    }

    private static Object convert(Object value, Class<?> targetType) {
        checkNotNull(targetType);
        if (value == null) {
            return null;
        }
        Class<?> wrappedType = wrap(targetType);
        if (wrappedType.isAssignableFrom(value.getClass())) {
            return wrappedType.cast(value);
        }
        if (wrappedType.isEnum()) {
            return stringToEnum(value.toString(), wrappedType);
        }
        if (wrappedType.equals(Integer.class)) {
            return stringToInteger(value.toString());
        } else if (wrappedType.equals(Long.class)) {
            return Long.valueOf(value.toString().trim());
        } else if (wrappedType.equals(Double.class)) {
            return stringToDouble(value.toString());
        } else if (wrappedType.equals(Float.class)) {
            return Float.valueOf(value.toString().trim());
        } else if (wrappedType.equals(Boolean.class)) {
            return Boolean.valueOf(value.toString());
        }
        throw new UnsupportedOperationException(format("Cannot convert from %s to %s. Requested converter does not"
                + " exist.", value.getClass().getName(), wrappedType.getName()));
    }

    private static Class<?> getRawClass(Type type, Field field) {
        if (type instanceof Class) {
            return (Class<?>) type;
        }
        throw new IllegalArgumentException(format("unsupported field-type %s for %s", type, field.getName()));
    }

    private static Class<?> wrap(Class<?> clazz) {
        if (clazz.equals(Integer.TYPE)) {
            return Integer.class;
        } else if (clazz.equals(Float.TYPE)) {
            return Float.class;
        } else if (clazz.equals(Double.TYPE)) {
            return Double.class;
        } else if (clazz.equals(Long.TYPE)) {
            return Long.class;
        } else if (clazz.equals(Boolean.TYPE)) {
            return Boolean.class;
        }
        return clazz;
    }

    private static Integer stringToInteger(String value) {
        String trimmedValue = value.trim();
        if (StringUtils.isEmpty(trimmedValue)) {
            return null;
        }
        return Integer.valueOf(trimmedValue);
    }

    private static Double stringToDouble(String value) {
        String trimmedValue = value.trim();
        if (StringUtils.isEmpty(trimmedValue)) {
            return null;
        }
        return Double.valueOf(trimmedValue);
    }

    private static <T> List<T> stringToList(String value, Class<T> type) {
        if (value == null) {
            return null;
        }
        return Arrays.stream(value.trim().split(LISTENER_DEL))
                .map(item -> type.cast(convert(item.trim(), type)))
                .collect(Collectors.toList());
    }

    private static <T> Set<T> stringToSet(String value, Class<T> type) {
        if (value == null) {
            return null;
        }
        return Arrays.stream(value.trim().split(LISTENER_DEL))
                .map(item -> type.cast(convert(item.trim(), type)))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static <K, V> Map<K, V> stringToMap(String value, Class<K> keyType, Class<V> valueType) {
        if (value == null) {
            return null;
        }
        return Arrays.stream(value.trim().split(LISTENER_DEL))
                .map(item -> item.trim().split("=", 2))
                .peek(parts -> checkArgument(parts.length == 2,
                        value + " map-value is not in correct format key1=value,key2=value2"))
                .collect(Collectors.toMap(
                        parts -> keyType.cast(convert(parts[0].trim(), keyType)),
                        parts -> valueType.cast(convert(parts[1].trim(), valueType)),
                        (oldValue, newValue) -> newValue));
    }

    private static Object stringToEnum(String value, Class<?> enumType) {
        String trimmedValue = value.trim();
        for (Object enumConstant : enumType.getEnumConstants()) {
            Enum<?> enumValue = (Enum<?>) enumConstant;
            if (enumValue.toString().equals(trimmedValue) || enumValue.name().equals(trimmedValue)) {
                return enumValue;
            }
        }
        throw new RuntimeException(format("Invalid value '%s' for enum %s", value, enumType));
    }


    public static int getListenerPort(String listener) {
        checkArgument(listener.matches(LISTENER_PATTERN), "listener not match pattern");

        int lastIndex = listener.lastIndexOf(':');
        return Integer.parseInt(listener.substring(lastIndex + 1));
    }

    public static int getProxyListenerPort(String listener) {
        checkArgument(listener.matches(PROXY_LISTENER_PATTERN), "proxy listener not match pattern");

        int lastIndex = listener.lastIndexOf(':');
        return Integer.parseInt(listener.substring(lastIndex + 1));
    }

    private ConfigurationUtils() {}

    /**
     * Parse PulsarConfiguration fields which with annotation @FieldContext
     * and put them into PulsarConfiguration's properties field.
     *
     * @param configuration
     * @throws IllegalAccessException
     */
    public static <T extends PulsarConfiguration> void extractFieldToProperties(T configuration)
            throws IllegalAccessException {
        Class<?> clazz = configuration.getClass();
        while (clazz != Object.class) {
            Field[] declaredFields = clazz.getDeclaredFields();
            for (Field field : declaredFields) {
                if (field.isAnnotationPresent(FieldContext.class)) {
                    field.setAccessible(true);
                    Object fieldValue = field.get(configuration);
                    String fieldName = field.getName();
                    if (fieldValue == null) {
                        // skip null field
                        continue;
                    }
                    String value = toConfigStr(fieldValue, field);
                    configuration.getProperties().put(fieldName, value);
                }
            }
            clazz = clazz.getSuperclass();
        }
    }

    private static String toConfigStr(Object fieldValue, Field field) {
        Type fieldType = field.getGenericType();
        // if field is not primitive type
        if (fieldType instanceof ParameterizedType) {
            if (field.getType().equals(List.class)) {
                return listToString(fieldValue);
            } else if (field.getType().equals(Set.class)) {
                return setToString(fieldValue);
            } else if (field.getType().equals(Map.class)) {
                return mapToString(fieldValue);
            } else if (field.getType().equals(Optional.class)) {
                Type typeClazz = ((ParameterizedType) fieldType).getActualTypeArguments()[0];
                if (typeClazz instanceof ParameterizedType) {
                    throw new IllegalArgumentException(format("unsupported non-primitive Optional<%s> for %s",
                            typeClazz.getClass(), field.getName()));
                } else {
                    Optional optional = (Optional) fieldValue;
                    if (optional.isPresent()) {
                        return optional.get().toString();
                    } else {
                        return StringUtils.EMPTY;
                    }
                }
            } else {
                throw new IllegalArgumentException(format("unsupported field-type %s for %s",
                        field.getType(), field.getName()));
            }
        } else {
            return fieldValue.toString();
        }
    }

    private static String mapToString(Object fieldValue) {
        Map map = (Map) fieldValue;
        List<String> kvList = new ArrayList<>(map.size());
        map.forEach(
                (k, v) -> {
                    String key = k.toString();
                    String value = v.toString();
                    kvList.add(key + "=" + value);
                }
        );
        return Joiner.on(LISTENER_DEL).join(kvList);
    }

    private static String setToString(Object fieldValue) {
        Set set = (Set) fieldValue;
        return Joiner.on(LISTENER_DEL).join(set);
    }

    private static String listToString(Object fieldValue) {
        List list = (List) fieldValue;
        return Joiner.on(LISTENER_DEL).join(list);
    }


}
