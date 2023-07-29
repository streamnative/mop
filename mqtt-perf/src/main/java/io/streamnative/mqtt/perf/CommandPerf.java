package io.streamnative.mqtt.perf;

import picocli.CommandLine;

@CommandLine.Command(name = "perf", version = "1.0.0", mixinStandardHelpOptions = true,
        subcommands = {CommandPub.class, CommandSub.class})
public final class CommandPerf implements Runnable {

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @Override
    public void run() {
        throw new CommandLine.ParameterException(spec.commandLine(), "Please specify the sub-command.");
    }
}
