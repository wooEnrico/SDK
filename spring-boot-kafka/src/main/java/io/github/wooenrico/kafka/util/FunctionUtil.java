package io.github.wooenrico.kafka.util;

import org.springframework.cloud.function.context.catalog.SimpleFunctionRegistry;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.Objects;

public final class FunctionUtil {

    private FunctionUtil() {
        // Utility class
    }

    public static boolean isConsumerOf(SimpleFunctionRegistry.FunctionInvocationWrapper wrapper, Class<?> targetRawType) {
        if (wrapper == null || !wrapper.isConsumer())
            return false;

        Type inputType = wrapper.getInputType();
        return isTypeAssignableTo(inputType, targetRawType);
    }

    public static boolean isFunctionOf(SimpleFunctionRegistry.FunctionInvocationWrapper wrapper, Class<?> inputRawType, Class<?> outputRawType) {
        if (wrapper == null || !wrapper.isFunction())
            return false;

        boolean inputOk = isTypeAssignableTo(wrapper.getInputType(), inputRawType);
        boolean outputOk = isTypeAssignableTo(wrapper.getOutputType(), outputRawType);

        return inputOk && outputOk;
    }

    public static boolean isSupplierOf(SimpleFunctionRegistry.FunctionInvocationWrapper wrapper, Class<?> outputRawType) {
        if (wrapper == null || !wrapper.isSupplier())
            return false;
        return isTypeAssignableTo(wrapper.getOutputType(), outputRawType);
    }

    private static boolean isTypeAssignableTo(Type type, Class<?> target) {
        Objects.requireNonNull(type, "Type must not be null");
        Objects.requireNonNull(target, "Target class must not be null");

        if (type instanceof Class) {
            return target.isAssignableFrom((Class<?>) type);
        }

        if (type instanceof ParameterizedType) {
            Type rawType = ((ParameterizedType) type).getRawType();
            if (rawType instanceof Class) {
                return target.isAssignableFrom((Class<?>) rawType);
            }
        }

        if (type instanceof WildcardType) {
            WildcardType wildcardType = (WildcardType) type;
            Type[] upperBounds = wildcardType.getUpperBounds();
            if (upperBounds.length > 0) {
                return isTypeAssignableTo(upperBounds[0], target);
            }
        }

        if (type instanceof GenericArrayType) {
            Type componentType = ((GenericArrayType) type).getGenericComponentType();
            return target.isAssignableFrom(Object[].class) && isTypeAssignableTo(componentType, Object.class);
        }

        return false;
    }
}
