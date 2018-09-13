package it.okkam.opendata.geonames.flink;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.TaskManagerOptions;

public class FlinkUtils {

  private FlinkUtils() {
    throw new IllegalArgumentException("Utility class");
  }

  private static final String FLINK_TMP_DIR = "/tmp";

  /**
   * Returns a pre-configured Execution env suitable for local tests from an IDE.
   * 
   * @return a suitable ExecutionEnvironment for local test from an IDE
   */
  public static ExecutionEnvironment getExecutionEnv() {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    if (env instanceof LocalEnvironment) {
      Configuration conf = new Configuration();
      conf.setString(CoreOptions.TMP_DIRS, FLINK_TMP_DIR);
      conf.setString(BlobServerOptions.STORAGE_DIRECTORY, FLINK_TMP_DIR);
      // conf.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 4); //NOSONAR
      // conf.setFloat(ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY, 0.4f);//NOSONAR
      // conf.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, 32768 * 2);//NOSONAR
      conf.setLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT, 32768 * 2);// NOSONAR
      env = ExecutionEnvironment.createLocalEnvironment(conf);
      env.setParallelism(Runtime.getRuntime().availableProcessors());
    }
    env.getConfig().disableGenericTypes();// NOSONAR
    return env;
  }

  // read everything as string
  public static TypeInformation<?>[] getDefaultFlinkFieldTypes(String[] fieldNames) {
    final TypeInformation<?>[] ret = new TypeInformation[fieldNames.length];
    for (int i = 0; i < ret.length; i++) {
      ret[i] = BasicTypeInfo.STRING_TYPE_INFO;
    }
    return ret;
  }

  public static RowTypeInfo getDefaultRowTypeInfo(String[] fieldNames) {
    return new RowTypeInfo(FlinkUtils.getDefaultFlinkFieldTypes(fieldNames));
  }

  public static int getFieldPos(String[] fieldNames, String targetField) {
    for (int i = 0; i < fieldNames.length; i++) {
      if (fieldNames[i].equals(targetField)) {
        return i;
      }
    }
    return -1;
  }
}
