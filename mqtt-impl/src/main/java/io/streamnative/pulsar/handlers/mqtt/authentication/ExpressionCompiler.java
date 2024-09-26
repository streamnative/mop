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
package io.streamnative.pulsar.handlers.mqtt.authentication;


import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.SimpleType;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerBuilder;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.parser.CelStandardMacro;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import java.util.Map;
import java.util.Set;
import lombok.Getter;

public class ExpressionCompiler {

    static final Set<String> DN_KEYS = Set.of("DC", "CN", "OU", "O", "L", "ST", "C", "UID");
    static final String DN = "DN";
    static final String SAN = "SAN";
    static final String SNID = "SNID";
    static final String SHA1 = "SHA1";
    static final CelCompiler COMPILER;

    static {
        CelCompilerBuilder celCompilerBuilder = CelCompilerFactory.standardCelCompilerBuilder()
            .setStandardMacros(CelStandardMacro.STANDARD_MACROS);
        celCompilerBuilder.addVar(DN, SimpleType.STRING);
        for (String key : DN_KEYS) {
            celCompilerBuilder.addVar(key, SimpleType.STRING);
        }
        celCompilerBuilder.addVar(SAN, SimpleType.STRING);
        celCompilerBuilder.addVar(SNID, SimpleType.STRING);
        celCompilerBuilder.addVar(SHA1, SimpleType.STRING);
        COMPILER = celCompilerBuilder.build();
    }

    final CelRuntime runtime = CelRuntimeFactory.standardCelRuntimeBuilder().build();

    @Getter
    private final String expression;

    private CelAbstractSyntaxTree ast;

    private CelRuntime.Program program;

    public ExpressionCompiler(String expression) {
        this.expression = expression;
        try {
            this.compile();
        } catch (CelValidationException | CelEvaluationException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    private void compile() throws CelValidationException, CelEvaluationException {
        this.ast = COMPILER.compile(expression).getAst();
        this.program = runtime.createProgram(ast);
    }

    public Boolean eval(Map<String, String> mapValue) throws Exception {
        final Object eval = program.eval(mapValue);
        if (eval instanceof Boolean) {
            return (Boolean) eval;
        }
        return false;
    }
}
