<template>
    <div class="app">
        <div ref="mainPlot"></div>
        <div ref="volPlot"></div>
        <div ref="oiPlot"></div>
        <!-- <p> 倒计时: {{ countdown }}</p> -->
        <!-- <button @click="launch">手动倒计时</button> -->
    </div>
</template>

<script setup>
import { ref, onMounted } from "vue"
import Plotly from "plotly.js-dist"


const countdown = ref(null)
const mainPlot = ref(null)
const volPlot = ref(null)
const oiPlot = ref(null)
const data = ref([])
const latest = ref({})
const today = ref(new Date().toISOString().split('T')[0])

const init = async () => {
    try {
        const url = new URL("/api/datetime", window.location.origin) // 拼接基础URL
        const response = await fetch(url)
        const dataRes = await response.json()
        today.value = dataRes.slice(0, 10)
    } catch (error) {
        console.error("获取datetime失败:", error)
    }
}
const launch = async () => {
    try {
        const tick = latest.value.ckpt
        const url = new URL("/api/test", window.location.origin) // 拼接基础URL
        url.searchParams.append("tick", tick) // 添加 tick 参数
        url.searchParams.append("symbol", "510300") // 添加 tick 参数
        const response = await fetch(url)
        const dataRes = await response.json()
        console.log(dataRes)
        countdown.value = dataRes.remain // + 1
        if (dataRes.data && dataRes.data.length > 0) { // 判断dataRes.data不为空且长度大于0
            data.value.push(...dataRes.data)
            latest.value = dataRes.latest
        }
        plot()
        console.log(`需要等待 ${countdown.value} 秒！`)
        if (countdown.value >= 0) {
            setTimeout(() => {
                console.log(`倒计时 ${countdown.value} 秒结束！`)
                launch()
            }, countdown.value * 1000)
        }
    } catch (error) {
        console.error("获取倒计时失败:", error)
    }
}
onMounted(() => {
    // init()
    launch()
})
const plot = () => {
    const unpack = (data, key) => data.map((item) => item[key])
    const withAxes = (trace, axis) => ({ ...trace, xaxis: axis, yaxis: axis })
    const duplicate = (trace) => [withAxes(trace, "x1"), withAxes(trace, "x2")]

    const pcrTrace = {
        mode: "lines",
        name: "berry",
        x: unpack(data.value, "ckpt"),
        y: unpack(data.value, "berry"),
        line: { color: "#2196f3" },
        // line: { color: Array(unpack(data.value, "ckpt").length).fill().map((_, i) => i < 600 ? "#c2bcb2" : "#9aadf6") },
    }
    const pcrMaTrace = {
        mode: "lines",
        name: "berry_ma",
        x: unpack(data.value, "ckpt"),
        y: unpack(data.value, "berry_ma"),
        line: { color: "#e2ecf780" },
    }
    const chgTrace = {
        mode: "lines",
        name: "chg",
        x: unpack(data.value, "ckpt"),
        y: unpack(data.value, "chg"),
        line: { color: "#000000" },
    }

    const volTrace = {
        type: 'bar',
        name: "vol",
        x: unpack(data.value, "ckpt"),
        y: unpack(data.value, "vol_inc"),
        line: { color: "#36a2eb" },
        marker: {
            color: unpack(data.value, "vol_inc").map(value => value > 10 ? "#673ab7" : "#e2ecf7")
        }
    }
    const oiTrace = {
        type: 'bar',
        name: "oi",
        x: unpack(data.value, "ckpt"),
        y: unpack(data.value, "oi_inc"),
        marker: {
            color: unpack(data.value, "oi_inc").map(value => {
                if (value > 2) {
                    return "#f44336"; // up 色
                } else if (value < -2) {
                    return "#4caf50"; // down 色
                } else {
                    return "#9d9d9d80"; // 灰色
                }
            })
        }
    }
    const oiCallTrace = {
        mode: "lines",
        name: "oi_call",
        x: unpack(data.value, "ckpt"),
        y: unpack(data.value, "oi_call"),
        line: { color: "#FF0000" },
    }
    const oiPutTrace = {
        mode: "lines",
        name: "oi_put",
        x: unpack(data.value, "ckpt"),
        y: unpack(data.value, "oi_put"),
        line: { color: "#228B22" },
    }
    const linechgTrace = {
        mode: "lines",
        name: "chgLine",
        x: [`${today.value} 09:30:00`, `${today.value} 15:00:00`],
        y: [latest.value.chg, latest.value.chg],
        line: { color: "#c9cbcf", dash: "dashdot" },
    }
    const linepcrTrace = {
        mode: "lines",
        name: "pcrLine",
        x: [`${today.value} 09:30:00`, `${today.value} 15:00:00`],
        y: [latest.value.berry, latest.value.berry],
        line: { color: "#c9cbcf", dash: "dashdot" },
    }
    const lineZenTrace = {
        mode: "lines",
        name: "zenLine",
        x: [`${today.value} 09:30:00`, `${today.value} 15:00:00`],
        y: [10, 10],
        line: { color: "#cc000080" },
    }

    const mainData = [
        oiTrace,
        pcrTrace,
        pcrMaTrace,
        chgTrace,
        linechgTrace,
        linepcrTrace,
    ].flatMap(duplicate)
    const volData = [volTrace, lineZenTrace].flatMap(duplicate)
    const oiData = [oiPutTrace, oiCallTrace].flatMap(duplicate)

    const baseLayout = {
        showlegend: false,
        grid: { rows: 1, columns: 2, pattern: "coupled", xgap: 0.05 },
        xaxis1: {
            range: [`${today.value} 09:30:00`, `${today.value} 11:30:00`],
        },
        xaxis2: {
            range: [`${today.value} 13:00:00`, `${today.value} 15:00:00`],
        },
        margin: {
            t: 10,
        },
        height: 200,
    }
    const mainLayout = {
        ...baseLayout,
        height: 300,
    }
    const volLayout = {
        ...baseLayout,
        yaxis: {
            range: [0, 16]
        },

    }
    const oiLayout = {
        ...baseLayout,
    }


    Plotly.newPlot(mainPlot.value, mainData, mainLayout)
    Plotly.newPlot(volPlot.value, volData, volLayout)
    Plotly.newPlot(oiPlot.value, oiData, oiLayout)
}
</script>

<style>
.app {
    text-align: center;
    margin-top: 50px;
}
</style>