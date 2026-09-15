package com.datastream.mvp.security;

import com.datastream.mvp.repository.AppUserRepository;
import io.jsonwebtoken.Claims;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.List;

/**
 * JWT 认证过滤器：解析 Authorization: Bearer <token> 并注入 SecurityContext
 */
@Component
@RequiredArgsConstructor
public class JwtAuthFilter extends OncePerRequestFilter {

    private final JwtUtil jwtUtil;
    private final AppUserRepository userRepo;

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        String header = request.getHeader("Authorization");
        if (header != null && header.startsWith("Bearer ")) {
            try {
                Claims claims = jwtUtil.parse(header.substring(7));
                Long userId = Long.valueOf(claims.getSubject());
                // JWT 只证明稳定身份；角色、名称与启用状态以数据库为准，修改后下一请求立即生效
                var user = userRepo.findById(userId).filter(com.datastream.mvp.model.AppUser::isEnabled)
                        .orElseThrow(() -> new IllegalStateException("用户不存在或已停用"));
                // 令牌版本校验：改密码 / 管理员重置密码 / 登出后自增，旧 token 立即失效
                if (!tokenVersionMatches(JwtUtil.tokenVersionOf(claims), user.getTokenVersion())) {
                    throw new IllegalStateException("令牌已失效，请重新登录");
                }
                String role = user.getRole().name();
                CurrentUser principal = new CurrentUser(userId, user.getUsername(), user.getDisplayName(), role);
                var auth = new UsernamePasswordAuthenticationToken(principal, null,
                        List.of(new SimpleGrantedAuthority("ROLE_" + role)));
                auth.setDetails(new WebAuthenticationDetailsSource().buildDetails(request));
                SecurityContextHolder.getContext().setAuthentication(auth);
            } catch (Exception e) {
                SecurityContextHolder.clearContext();
            }
        }
        filterChain.doFilter(request, response);
    }

    /**
     * 令牌版本是否匹配：token 里的版本必须等于库中当前版本。
     * claim 缺失按 0 处理（向后兼容旧 token）；库中为 null 也按 0 处理。
     */
    static boolean tokenVersionMatches(Integer tokenVersion, Integer currentVersion) {
        int token = tokenVersion == null ? 0 : tokenVersion;
        int current = currentVersion == null ? 0 : currentVersion;
        return token == current;
    }
}
