// use tonic::Request;
// use vr::replica::MyViewStamped;
// use vr::view_stamped::view_stamped_server::ViewStampedServer;
// use vr::view_stamped::RequestMessage;
// use vr::replica::view_stamped::view_stamped_server::ViewStamped;

// #[tokio::test]
// async fn test_vr() {

//     let addr = "[::1]:50051".parse().unwrap();
//     let vr = MyViewStamped::default();

//     let server = tonic::transport::Server::builder()
//         .add_service(ViewStampedServer::new(vr))
//         .serve(addr);

//     println!("VR replica listening on {}", addr);

//     //Simulate a client request
//     let request = Request::new(RequestMessage {
//         request: Some(vr::view_stamped::Request {
//             op: "AddT".into(),
//             clientid: 45,
//             clientreqid: 11211,
//         }),
//     });

//     let response = vr
//         .view_stamped_request(request)
//         .await
//         .unwrap();

//     //Assert that the response is correct
//     assert_eq!(response.into_inner().reply, "Processed");
// }