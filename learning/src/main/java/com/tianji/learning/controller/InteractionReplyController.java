package com.tianji.learning.controller;


import com.tianji.common.domain.dto.PageDTO;
import com.tianji.learning.domain.dto.ReplyDTO;
import com.tianji.learning.domain.query.ReplyPageQuery;
import com.tianji.learning.domain.vo.ReplyVO;
import com.tianji.learning.service.IInteractionReplyService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

/**
 * <p>
 * 互动问题的回答或评论 前端控制器
 * </p>
 *
 * @author lusy
 * @since 2025-03-11
 */
@Api(tags = "评论相关接口")
@RequiredArgsConstructor
@RestController
@RequestMapping("/replies")
public class InteractionReplyController {

    private final IInteractionReplyService interactionReplyService;

    @ApiOperation("新增评论")
    @PostMapping()
    public void saveReply(@RequestBody ReplyDTO replyDTO) {
        interactionReplyService.saveReply(replyDTO);
    }

    @ApiOperation("分页查询回答or评论")
    @GetMapping("/page")
    public PageDTO<ReplyVO> pageReply(ReplyPageQuery query) {
        return interactionReplyService.pageReply(query);
    }

}
