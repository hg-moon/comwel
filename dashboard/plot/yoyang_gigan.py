import numpy as np
import pandas as pd

import plotly.express as px


def fig_yoyang_gigan_by_wonbu_no_plus_pred_sincheong_gigan(wonbu_no_data, data, col, sincheong_ilsu, pred, percentile):

    ####
    percentile_line = int(np.percentile(data[col], float(percentile)))
    #median = int(data[col].median())
    max_value = int(data[col].max())
    wonbu_no_yoyang_ilsu = int(wonbu_no_data[col].item())
    wonbu_no_yoyang_ilsu_plus_sincheong_ilsu = wonbu_no_yoyang_ilsu + sincheong_ilsu
    ####

    ##fig
    fig = px.violin(data, x=col, template='plotly') #fig = px.violin(data, x=col, box=True, template='plotly')

    fig.update_layout(width = 600, height=375)

    fig.update_traces(side='positive', spanmode='hard', meanline_visible=False, hovertemplate=None, hoverinfo="skip", marker_color="grey")

    fig.update_layout(margin=dict(t=65, b=0, l=15, r=15),
                    xaxis_title="요양기간 (일)", font=dict(size=12))

    fig.update_xaxes(tickfont=dict(size=16))
    fig.update_yaxes(tickfont=dict(size=16))

    #init
    if percentile == "50":
        fig.add_vline(x=percentile_line, line_color="blue", line_width=3)

        fig.add_annotation(dict(font=dict(color="blue",size=16),
                                    x=percentile_line,
                                    y=1.1,
                                    showarrow=False,
                                    text= f"<b> 중앙값 : {percentile_line} 일 </b>",
                                    textangle=0,
                                    xref="x",
                                    yref="paper"
                                ))
    else:
        fig.add_vline(x=percentile_line, line_color="blue", line_width=3)

        fig.add_annotation(dict(font=dict(color="blue",size=16),
                                    x=percentile_line,
                                    y=1.1,
                                    showarrow=False,
                                    text= f"<b> {percentile} % : {percentile_line} 일 </b>",
                                    textangle=0,
                                    xref="x",
                                    yref="paper"
                                ))        



    fig.add_vline(x=wonbu_no_yoyang_ilsu, line_color="black", line_width=2, line_dash="dash")

    fig.add_vline(x=wonbu_no_yoyang_ilsu_plus_sincheong_ilsu, line_color="#F63366", line_width=3)



    fig.add_annotation(dict(font=dict(color="black",size=16),
                                x=wonbu_no_yoyang_ilsu,
                                y=1.17,
                                showarrow=False,
                                text='승인 : ' + str(wonbu_no_yoyang_ilsu) + ' 일',
                                textangle=0,
                                xref="x",
                                yref="paper"
                            ))

    fig.add_annotation(dict(font=dict(color="#F63366",size=16),
                                x=wonbu_no_yoyang_ilsu_plus_sincheong_ilsu,
                                y=1.25,
                                showarrow=False,
                                text='<b> 승인+신청 : ' + str(wonbu_no_yoyang_ilsu_plus_sincheong_ilsu) + ' 일 </b>',
                                textangle=0,
                                xref="x",
                                yref="paper"
                            ))

    if pred != "none":
        #fig.add_vline(x=pred, line_color="#ff7f0e", line_width=3)
        
        fig.add_annotation(dict(font=dict(color="black",size=16),
                                    x=max_value, #pred
                                    y=1.0, #1.32
                                    showarrow=False,
                                    text='* 🤖 AI 예측 : ' + str(pred) + ' 일',
                                    textangle=0,
                                    xref="x",
                                    yref="paper"
                                ))

    return fig


def fig_yoyang_gigan_by_wonbu_no_sangse_plus_pred_sincheong_gigan(wonbu_no_data, data, col, sincheong_ilsu, pred, percentile):

    ####
    percentile_line = int(np.percentile(data[col], float(percentile)))
    #median = int(data[col].median())
    max_value = int(data[col].max())
    wonbu_no_yoyang_ilsu = int(wonbu_no_data[col].item())
    wonbu_no_yoyang_ilsu_plus_sincheong_ilsu = wonbu_no_yoyang_ilsu + sincheong_ilsu
    ####

    ##fig
    fig = px.histogram(data, x=col, template='plotly', color_discrete_sequence=['grey']) #fig = px.histogram(data, x=col, marginal='box', template='plotly')

    fig.update_layout(width = 600, height=375)

    fig.update_layout(bargap=0.2)
    fig.update_layout(margin=dict(t=65, b=0, l=15, r=15),
                    xaxis_title="요양기간 (일)",
                    yaxis_title="재해자 수 (명)",
                    font=dict(size=12),
                    hovermode="x")

    fig.update_xaxes(tickfont=dict(size=16))
    fig.update_yaxes(tickfont=dict(size=16))

    fig.update_traces(marker_line_width=2, marker_line_color='black', marker_opacity =0.5,
                    hovertemplate=" 요양기간: %{x} 일 <br> 재해자수: %{y} 명", hoverlabel=dict(bgcolor='rgba(255,255,255,0.75)',font=dict(color='black')))


    #init
    if percentile == "50":
        fig.add_vline(x=percentile_line, line_color="blue", line_width=3)

        fig.add_annotation(dict(font=dict(color="blue",size=16),
                                    x=percentile_line,
                                    y=1.1,
                                    showarrow=False,
                                    text= f"<b> 중앙값 : {percentile_line} 일 </b>",
                                    textangle=0,
                                    xref="x",
                                    yref="paper"
                                ))
    else:
        fig.add_vline(x=percentile_line, line_color="blue", line_width=3)

        fig.add_annotation(dict(font=dict(color="blue",size=16),
                                    x=percentile_line,
                                    y=1.1,
                                    showarrow=False,
                                    text= f"<b> {percentile} % : {percentile_line} 일 </b>",
                                    textangle=0,
                                    xref="x",
                                    yref="paper"
                                )) 

    fig.add_vline(x=wonbu_no_yoyang_ilsu, line_color="black",line_dash="dash", line_width=2)

    fig.add_vline(x=wonbu_no_yoyang_ilsu_plus_sincheong_ilsu, line_color="#F63366", line_width=3)

    fig.add_annotation(dict(font=dict(color="black",size=16),
                                x=wonbu_no_yoyang_ilsu,
                                y=1.17,
                                showarrow=False,
                                text='승인 : ' + str(wonbu_no_yoyang_ilsu) + ' 일',
                                textangle=0,
                                xref="x",
                                yref="paper"
                            ))

    fig.add_annotation(dict(font=dict(color="#F63366",size=16),
                                x=wonbu_no_yoyang_ilsu_plus_sincheong_ilsu,
                                y=1.25,
                                showarrow=False,
                                text='<b> 승인+신청 : ' + str(wonbu_no_yoyang_ilsu_plus_sincheong_ilsu) + ' 일 <b>',
                                textangle=0,
                                xref="x",
                                yref="paper"
                            ))

    if pred != "none":
        #fig.add_vline(x=pred, line_color="#ff7f0e", line_width=3)
        
        fig.add_annotation(dict(font=dict(color="black",size=16),
                                    x=max_value, #pred
                                    y=1.0, #1.32
                                    showarrow=False,
                                    text='* 🤖 AI 예측 : ' + str(pred) + ' 일',
                                    textangle=0,
                                    xref="x",
                                    yref="paper"
                                ))

    return fig





'''
def fig_yoyang_gigan_by_wonbu_no_plus_sincheong_gigan(wonbu_no_data, data, col, sincheong_ilsu, percentile):

    ####
    percentile_line = int(np.percentile(data[col], float(percentile)))
    #median = int(data[col].median())
    wonbu_no_yoyang_ilsu = int(wonbu_no_data[col].item())
    wonbu_no_yoyang_ilsu_plus_sincheong_ilsu = wonbu_no_yoyang_ilsu + sincheong_ilsu
    ####

    ##fig
    fig = px.violin(data, x=col, template='plotly') #fig = px.violin(data, x=col, box=True, template='plotly')

    fig.update_layout(width = 600, height=350)

    fig.update_traces(side='positive', spanmode='hard', meanline_visible=False, hovertemplate=None, hoverinfo="skip", marker_color="grey")

    fig.update_layout(margin=dict(t=60, b=0, l=15, r=15),
                    xaxis_title="요양기간 (일)", font=dict(size=12))

    #init
    if percentile == "50":
        fig.add_vline(x=percentile_line, line_color="blue", line_width=3)

        fig.add_annotation(dict(font=dict(color="blue",size=16),
                                    x=percentile_line,
                                    y=1.1,
                                    showarrow=False,
                                    text= f"중앙값 : {percentile_line} 일",
                                    textangle=0,
                                    xref="x",
                                    yref="paper"
                                ))
    else:
        fig.add_vline(x=percentile_line, line_color="blue", line_width=3)

        fig.add_annotation(dict(font=dict(color="blue",size=16),
                                    x=percentile_line,
                                    y=1.1,
                                    showarrow=False,
                                    text= f"{percentile} % : {percentile_line} 일",
                                    textangle=0,
                                    xref="x",
                                    yref="paper"
                                ))        



    fig.add_vline(x=wonbu_no_yoyang_ilsu, line_color="black", line_width=2, line_dash="dash")

    fig.add_vline(x=wonbu_no_yoyang_ilsu_plus_sincheong_ilsu, line_color="#F63366", line_width=3)



    fig.add_annotation(dict(font=dict(color="black",size=16),
                                x=wonbu_no_yoyang_ilsu,
                                y=1.17,
                                showarrow=False,
                                text='승인 : ' + str(wonbu_no_yoyang_ilsu) + ' 일',
                                textangle=0,
                                xref="x",
                                yref="paper"
                            ))

    fig.add_annotation(dict(font=dict(color="#F63366",size=16),
                                x=wonbu_no_yoyang_ilsu_plus_sincheong_ilsu,
                                y=1.25,
                                showarrow=False,
                                text='승인+신청 : ' + str(wonbu_no_yoyang_ilsu_plus_sincheong_ilsu) + ' 일',
                                textangle=0,
                                xref="x",
                                yref="paper"
                            ))

    return fig
'''




'''
#신청요양기간 반영
def fig_yoyang_gigan_by_wonbu_no_sangse_plus_sincheong_gigan(wonbu_no_data, data, col, sincheong_ilsu, percentile):

    ####
    percentile_line = int(np.percentile(data[col], float(percentile)))
    #median = int(data[col].median())
    wonbu_no_yoyang_ilsu = int(wonbu_no_data[col].item())
    wonbu_no_yoyang_ilsu_plus_sincheong_ilsu = wonbu_no_yoyang_ilsu + sincheong_ilsu
    ####

    ##fig
    fig = px.histogram(data, x=col, template='plotly', color_discrete_sequence=['grey'])

    fig.update_layout(width = 600, height=350)
    
    fig.update_layout(bargap=0.2)
    fig.update_layout(margin=dict(t=60, b=0, l=15, r=15),
                    xaxis_title="요양기간 (일)",
                    yaxis_title="재해자 수 (명)",
                    font=dict(size=12),
                    hovermode="x")

    fig.update_traces(marker_line_width=2, marker_line_color='black', marker_opacity =0.5,
                    hovertemplate=" 요양기간: %{x} 일 <br> 재해자수: %{y} 명", hoverlabel=dict(bgcolor='rgba(255,255,255,0.75)',font=dict(color='black')))


    #init
    if percentile == "50":
        fig.add_vline(x=percentile_line, line_color="blue", line_width=3)

        fig.add_annotation(dict(font=dict(color="blue",size=16),
                                    x=percentile_line,
                                    y=1.1,
                                    showarrow=False,
                                    text= f"중앙값 : {percentile_line} 일",
                                    textangle=0,
                                    xref="x",
                                    yref="paper"
                                ))
    else:
        fig.add_vline(x=percentile_line, line_color="blue", line_width=3)

        fig.add_annotation(dict(font=dict(color="blue",size=16),
                                    x=percentile_line,
                                    y=1.1,
                                    showarrow=False,
                                    text= f"{percentile} % : {percentile_line} 일",
                                    textangle=0,
                                    xref="x",
                                    yref="paper"
                                ))   


    fig.add_vline(x=wonbu_no_yoyang_ilsu, line_color="black", line_dash="dash", line_width=2) ##7f7f7f, #696969

    fig.add_vline(x=wonbu_no_yoyang_ilsu_plus_sincheong_ilsu, line_color="#F63366", line_width=3)



    fig.add_annotation(dict(font=dict(color="black",size=16),
                                x=wonbu_no_yoyang_ilsu,
                                y=1.17,
                                showarrow=False,
                                text='승인 : ' + str(wonbu_no_yoyang_ilsu) + ' 일',
                                textangle=0,
                                xref="x",
                                yref="paper"
                            ))

    fig.add_annotation(dict(font=dict(color="#F63366",size=16),
                                x=wonbu_no_yoyang_ilsu_plus_sincheong_ilsu,
                                y=1.25,
                                showarrow=False,
                                text='승인+신청 : ' + str(wonbu_no_yoyang_ilsu_plus_sincheong_ilsu) + ' 일',
                                textangle=0,
                                xref="x",
                                yref="paper"
                            ))

    return fig
'''
