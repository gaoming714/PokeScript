<template></template>

<script setup>
import { ref, onMounted, onUnmounted } from "vue";
import { io } from "socket.io-client";
import { useNotification } from "naive-ui";

// 数据状态
const data = ref(null);

const notification = useNotification();

// 定义 notify 函数
const notify = (type, title, msg) => {
  notification[type]({
    content: title,
    meta: msg,
    duration: 12000,
    keepAliveOnHover: true,
  });
};

// WebSocket 连接
let socket;

onMounted(() => {
  // 初始化 WebSocket 连接
  socket = io("/");

  // 监听服务器推送的 'update_data' 事件
  socket.on("update_data", (newData) => {
    console.log("收到数据:", newData);
    data.value = newData.data; // 更新数据
    // snackbar.value = true;
    notify("info", newData.data.title, newData.data.msg);
  });

  socket.on("connect", () => {
    console.log("WebSocket 连接成功");
  });

  socket.on("disconnect", () => {
    console.log("WebSocket 连接断开");
  });
  socket.on("connect_error", (err) => console.log("连接错误:", err)); // 添加错误日志
});

onUnmounted(() => {
  if (socket) {
    socket.disconnect();
  }
});
</script>

<style scoped></style>
