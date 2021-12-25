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
package io.streamnative.pulsar.handlers.mqtt.utils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReflectionUtils {

    public static Method reflectAccessibleMethod(Class<?> objType, String methodName) {
        try {
            Method method = objType.getMethod(methodName);
            method.setAccessible(true);
            return method;
        } catch (NoSuchMethodException ex) {
            log.error("Reflect {} method {} got error,", objType, methodName, ex);
            throw new IllegalArgumentException(ex);
        }
    }

    public static Reference createReference(Method method, Object obj) {
        return new Reference(method, obj);
    }

    public static class Reference {
        private final Method method;
        private final Object obj;

        public Reference(Method method, Object obj) {
            this.method = method;
            this.obj = obj;
        }

        public Object invoke() {
            try {
                return method.invoke(obj);
            } catch (IllegalAccessException | InvocationTargetException e) {
                log.error("Reflect {} method {} got error,", obj.getClass(), method.getName(), e);
                throw new IllegalStateException(e);
            }
        }
    }
}
