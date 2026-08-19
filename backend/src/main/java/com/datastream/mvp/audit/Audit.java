package com.datastream.mvp.audit;

import java.lang.annotation.*;

/**
 * 操作审计注解：标注在 Controller 方法上，方法成功返回后记录审计日志
 * targetId / detail 支持 SpEL（可用方法参数名与 #result）
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Audit {
    String action();
    String targetType();
    String targetId() default "";
    String detail() default "";
}
