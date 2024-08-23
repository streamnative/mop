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
package io.streamnative.pulsar.handlers.mqtt.oidc;

import com.google.common.annotations.VisibleForTesting;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelValidationException;
import dev.cel.common.CelVarDecl;
import dev.cel.common.types.MapType;
import dev.cel.common.types.SimpleType;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.parser.CelStandardMacro;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExpressionCompiler {

    @Getter
    private CelCompiler compiler = CelCompilerFactory.standardCelCompilerBuilder()
                                        .setStandardMacros(CelStandardMacro.STANDARD_MACROS)
                                        .addVarDeclarations(CelVarDecl.newVarDeclaration("claims",
                                            MapType.create(SimpleType.STRING, SimpleType.STRING)))
                                        .build();

    final CelRuntime runtime = CelRuntimeFactory.standardCelRuntimeBuilder().build();

    static final String REGEX = "claims[\\w.]*";

    static final Pattern PATTERN = Pattern.compile(REGEX);

    @Getter
    private String expression;

    @Getter
    private List<String> variables;

    private CelAbstractSyntaxTree ast;

    private CelRuntime.Program program;

    public ExpressionCompiler(String expression) {
        this.expression = expression;
        this.variables = parse(expression);
        try {
            this.compile();
        } catch (CelValidationException | CelEvaluationException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    @VisibleForTesting
    ExpressionCompiler() {
    }

    @VisibleForTesting
    List<String> parse(String expression) {
        if (StringUtils.isEmpty(expression)) {
            throw new IllegalArgumentException("Expression should not be empty");
        }
        Matcher matcher = PATTERN.matcher(expression);
        List<String> matches = new ArrayList<>();
        while (matcher.find()) {
            String find = matcher.group(0);
            String[] parts = find.split("\\.");
            matches.add(parts[1]);
        }
        if (CollectionUtils.isEmpty(matches)) {
            throw new IllegalArgumentException("Not valid expression, "
                    + "expression definitions must be prefixed with claims.");
        }
        return matches;
    }

    private void compile() throws CelValidationException, CelEvaluationException {
        this.ast = compiler.compile(expression).getAst();
        this.program = runtime.createProgram(ast);
    }

    public Boolean eval(Map<String, Object> mapValue) throws Exception {
        final Object eval = program.eval(mapValue);
        if (eval instanceof Boolean) {
            return (Boolean) eval;
        }
        return false;
    }
}
