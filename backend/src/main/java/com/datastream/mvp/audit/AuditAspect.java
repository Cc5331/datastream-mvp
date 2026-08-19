package com.datastream.mvp.audit;

import com.datastream.mvp.security.CurrentUser;
import com.datastream.mvp.security.SecurityUtils;
import com.datastream.mvp.service.AuditService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * @Audit 切面：方法成功执行后写入审计日志（失败不阻断业务）
 */
@Slf4j
@Aspect
@Component
@RequiredArgsConstructor
public class AuditAspect {

    private final AuditService auditService;
    private final SpelExpressionParser parser = new SpelExpressionParser();

    @Around("@annotation(audit)")
    public Object around(ProceedingJoinPoint pjp, Audit audit) throws Throwable {
        Object result = pjp.proceed();
        try {
            String targetId = eval(audit.targetId(), pjp, result);
            String detail = eval(audit.detail(), pjp, result);
            CurrentUser cu = SecurityUtils.currentUser();
            auditService.record(
                    cu != null ? cu.id() : null,
                    cu != null ? cu.username() : "system",
                    audit.action(),
                    audit.targetType(),
                    targetId,
                    detail,
                    requestIp());
        } catch (Exception e) {
            log.warn("Audit record failed: {}", e.getMessage());
        }
        return result;
    }

    private String eval(String spel, ProceedingJoinPoint pjp, Object result) {
        if (spel == null || spel.isBlank()) return "";
        try {
            StandardEvaluationContext ctx = new StandardEvaluationContext();
            ctx.setVariable("result", result);
            MethodSignature sig = (MethodSignature) pjp.getSignature();
            Object[] args = pjp.getArgs();
            String[] names = sig.getParameterNames();
            if (names != null) {
                for (int i = 0; i < names.length && i < args.length; i++) {
                    ctx.setVariable(names[i], args[i]);
                }
            }
            Object value = parser.parseExpression(spel).getValue(ctx);
            return value == null ? "" : String.valueOf(value);
        } catch (Exception e) {
            return "";
        }
    }

    private String requestIp() {
        try {
            ServletRequestAttributes attrs = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
            if (attrs != null && attrs.getRequest() != null) return attrs.getRequest().getRemoteAddr();
        } catch (Exception ignored) {}
        return "";
    }
}
